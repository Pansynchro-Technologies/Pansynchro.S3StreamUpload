using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Amazon.S3;
using Amazon.S3.Model;
using log4net;

namespace Pansynchro.S3StreamUpload
{
	/// <summary>
	/// Manages streaming of data to S3 without knowing the size beforehand and without keeping it all in memory or
	/// writing to disk.
	/// <para>
	/// The data is split into chunks and uploaded using the multipart upload API by one or more separate threads.
	/// </para>
	/// <para>
	/// After creating an instance with details of the upload, use <seealso cref="StreamTransferManager.getMultiPartOutputStreams()"/>
	/// to get a list
	/// of <seealso cref="MultiPartOutputStream"/>s. When you finish writing data, call <seealso cref="MultiPartOutputStream.close()"/>.
	/// Parts will be uploaded to S3 as you write.
	/// </para>
	/// <para>
	/// Once all streams have been closed, call <seealso cref="StreamTransferManager.Complete()"/>. Alternatively you can call
	/// <seealso cref="StreamTransferManager.Abort()"/>
	/// at any point if needed.
	/// </para>
	/// <para>
	/// Here is an example. A lot of the code relates to setting up threads for creating data unrelated to the library. The
	/// essential parts are commented.
	/// <pre>{@code
	///    IAmazonS3Client client = new IAmazonS3Client(awsCreds);
	/// 
	///    // Setting up
	///    int numStreams = 2;
	///    final StreamTransferManager manager = new StreamTransferManager(bucket, key, client)
	///            .numStreams(numStreams)
	///            .numUploadThreads(2)
	///            .queueCapacity(2)
	///            .partSize(10);
	///    final List<MultiPartOutputStream> streams = manager.getMultiPartOutputStreams();
	/// 
	///    ExecutorService pool = Executors.newFixedThreadPool(numStreams);
	///    for (int i = 0; i < numStreams; i++) {
	///        final int streamIndex = i;
	///        pool.submit(new Runnable() {
	///            public void run() {
	///                try {
	///                    MultiPartOutputStream outputStream = streams.get(streamIndex);
	///                    for (int lineNum = 0; lineNum < 1000000; lineNum++) {
	///                        String line = generateData(streamIndex, lineNum);
	/// 
	///                        // Writing data and potentially sending off a part
	///                        outputStream.write(line.getBytes());
	///                    }
	/// 
	///                    // The stream must be closed once all the data has been written
	///                    outputStream.close();
	///                } catch (Exception e) {
	/// 
	///                    // Aborts all uploads
	///                    manager.abort(e);
	///                }
	///            }
	///        });
	///    }
	///    pool.shutdown();
	///    pool.awaitTermination(5, TimeUnit.SECONDS);
	/// 
	///    // Finishing off
	///    manager.complete();
	/// }</pre>
	/// </para>
	/// <para>
	/// The final file on S3 will then usually be the result of concatenating all the data written to each stream,
	/// in the order that the streams were in in the list obtained from {@code getMultiPartOutputStreams()}. However this
	/// may not be true if multiple streams are used and some of them produce less than 5 MB of data. This is because the multipart
	/// upload API does not allow the uploading of more than one part smaller than 5 MB, which leads to fundamental limits
	/// on what this class can accomplish. If order of data is important to you, then either use only one stream or ensure
	/// that you write at least 5 MB to every stream.
	/// </para>
	/// <para>
	/// While performing the multipart upload this class will create instances of <seealso cref="InitiateMultipartUploadRequest"/>,
	/// <seealso cref="UploadPartRequest"/>, and <seealso cref="CompleteMultipartUploadRequest"/>, fill in the essential details, and send them
	/// off. If you need to add additional details then override the appropriate {@code customise*Request} methods and
	/// set the required properties within. Note that if no data is written (i.e. the object body is empty) then a normal (not multipart) upload will be performed and {@code customisePutEmptyObjectRequest} will be called instead.
	/// </para>
	/// <para>
	/// This class does not perform retries when uploading. If an exception is thrown at any stage the upload will be aborted and the
	/// exception rethrown, wrapped in a {@code RuntimeException}.
	/// </para>
	/// <para>
	/// You can configure the upload process by calling any of the chaining setter methods <seealso cref="StreamTransferManager.NumStreams(int)"/>, <seealso cref="StreamTransferManager.NumUploadThreads(int)"/>, <seealso cref="StreamTransferManager.QueueCapacity(int)"/>, or <seealso cref="StreamTransferManager.PartSize(long)"/> before calling {@code getMultiPartOutputStreams}. Parts that have been produced sit in a queue of specified capacity while they wait for a thread to upload them.
	/// The worst case memory usage is {@code (numUploadThreads + queueCapacity) * partSize + numStreams * (partSize + 6MB)},
	/// while higher values for these first three parameters may lead to better resource usage and throughput.
	/// If you are uploading very large files, you may need to increase the part size - see <seealso cref="StreamTransferManager.PartSize(long)"/> for details.
	/// 
	/// @author Alex Hall
	/// </para>
	/// </summary>
	public class StreamTransferManager
	{
		private static readonly ILog _log = LogManager.GetLogger(typeof(StreamTransferManager));
		private const int MB = 1024 * 1024;

		protected internal readonly string _bucketName;
		protected internal readonly string _putKey;
		protected internal readonly IAmazonS3 _s3Client;
		protected internal string? _uploadId;
		protected internal int _numStreams = 1;
		protected internal int _numUploadThreads = 1;
		protected internal int _queueCapacity = 1;
		protected internal int _partSize = 5 * MB;
		protected internal bool _checkIntegrity = false;
		private readonly List<PartETag> _partETags = new();
		private IList<MultiPartOutputStream>? _multiPartOutputStreams;
		private readonly List<Task> _uploadTasks = new();
		private readonly CancellationTokenSource _cancellationTokenSource = new();
		private BlockingCollection<StreamPart>? _queue;
		private int _finishedCount = 0;
		private StreamPart? _leftoverStreamPart = null;
		private readonly object _leftoverStreamPartLock = new();
		private bool _isAborting = false;
		private const int MAX_PART_NUMBER = 10000;

		public StreamTransferManager(string bucketName, string putKey, IAmazonS3 s3Client)
		{
			_bucketName = bucketName;
			_putKey = putKey;
			_s3Client = s3Client;
		}

		/// <summary>
		/// Sets the number of <seealso cref="MultiPartOutputStream"/>s that will be created and returned by
		/// <seealso cref="StreamTransferManager.getMultiPartOutputStreams()"/> for you to write to.
		/// <para>
		/// By default this is 1, increase it if you want to write to multiple streams from different
		/// threads in parallel.
		/// </para>
		/// <para>
		/// If you are writing large files with many streams, you may need to increase the part size
		/// to avoid running out of part numbers - see <seealso cref="StreamTransferManager.PartSize(long)"/>
		/// for more details.
		/// </para>
		/// <para>
		/// Each stream may hold up to <seealso cref="StreamTransferManager.PartSize(long)"/> + 6MB
		/// in memory at a time.
		/// 
		/// </para>
		/// </summary>
		/// <returns> this {@code StreamTransferManager} for chaining. </returns>
		/// <exception cref="IllegalArgumentException"> if the argument is less than 1. </exception>
		/// <exception cref="IllegalStateException">    if <seealso cref="StreamTransferManager.getMultiPartOutputStreams"/> has already
		///                                  been called, initiating the upload. </exception>
		public virtual StreamTransferManager NumStreams(int numStreams)
		{
			EnsureCanSet();
			if (numStreams < 1)
			{
				throw new ArgumentException("There must be at least one stream");
			}
			_numStreams = numStreams;
			return this;
		}

		/// <summary>
		/// Sets the number of threads that will be created to upload the data in parallel to S3.
		/// <para>
		/// By default this is 1, increase it if uploading is a speed bottleneck and you have network
		/// bandwidth to spare.
		/// </para>
		/// <para>
		/// Each thread may hold up to <seealso cref="StreamTransferManager.PartSize(long)"/>
		/// in memory at a time.
		/// 
		/// </para>
		/// </summary>
		/// <returns> this {@code StreamTransferManager} for chaining. </returns>
		/// <exception cref="IllegalArgumentException"> if the argument is less than 1. </exception>
		/// <exception cref="IllegalStateException">    if <seealso cref="StreamTransferManager.getMultiPartOutputStreams"/> has already
		///                                  been called, initiating the upload. </exception>
		public virtual StreamTransferManager NumUploadThreads(int numUploadThreads)
		{
			EnsureCanSet();
			if (numUploadThreads < 1)
			{
				throw new ArgumentException("There must be at least one upload thread");
			}
			_numUploadThreads = numUploadThreads;
			return this;
		}

		/// <summary>
		/// Sets the capacity of the queue where completed parts from the output streams will sit
		/// waiting to be taken by the upload threads.
		/// <para>
		/// By default this is 1, increase it if you want to help your threads which write
		/// to the streams be consistently busy instead of blocking waiting for upload threads.
		/// </para>
		/// <para>
		/// Each part sitting in the queue will hold <seealso cref="StreamTransferManager.PartSize(long)"/> bytes
		/// in memory at a time.
		/// 
		/// </para>
		/// </summary>
		/// <returns> this {@code StreamTransferManager} for chaining. </returns>
		/// <exception cref="IllegalArgumentException"> if the argument is less than 1. </exception>
		/// <exception cref="IllegalStateException">    if <seealso cref="StreamTransferManager.getMultiPartOutputStreams"/> has already
		///                                  been called, initiating the upload. </exception>
		public virtual StreamTransferManager QueueCapacity(int queueCapacity)
		{
			EnsureCanSet();
			if (queueCapacity < 1)
			{
				throw new ArgumentException("The queue capacity must be at least 1");
			}
			_queueCapacity = queueCapacity;
			return this;
		}

		/// <summary>
		/// Sets the size in MB of the parts to be uploaded to S3.
		/// <para>
		/// By default this is 5, which is the minimum that AWS allows. You may need to increase
		/// it if you are uploading very large files or writing to many output streams.
		/// </para>
		/// <para>
		/// AWS allows up to 10,000 parts to be uploaded for a single object, and each part must be
		/// identified by a unique number from 1 to 10,000. These part numbers are allocated evenly
		/// by the manager to each output stream. Therefore the maximum amount of data that can be
		/// written to a stream is {@code 10000/numStreams * partSize}. If you try to write more,
		/// an {@code IndexOutOfBoundsException} will be thrown.
		/// The total object size can be at most 5 TB, so if you're using just one stream,
		/// there is no reason to set this higher than 525. If you're using more streams, you may want
		/// a higher value in case some streams get more data than others.
		/// </para>
		/// <para>
		/// Increasing this value will of course increase memory usage.
		/// 
		/// </para>
		/// </summary>
		/// <returns> this {@code StreamTransferManager} for chaining. </returns>
		/// <exception cref="IllegalArgumentException"> if the argument is less than 5. </exception>
		/// <exception cref="IllegalArgumentException"> if the resulting part size in bytes cannot fit in a 32 bit int. </exception>
		/// <exception cref="IllegalStateException">    if <seealso cref="StreamTransferManager.getMultiPartOutputStreams"/> has already
		///                                  been called, initiating the upload. </exception>
		// partSize is a long here in case of a mistake on the user's part before calling this method.
		public virtual StreamTransferManager PartSize(long partSize)
		{
			EnsureCanSet();
			partSize *= MB;
			if (partSize < MultiPartOutputStream.S3_MIN_PART_SIZE)
			{
				throw new ArgumentException($"The given part size ({partSize}) is less than 5 MB.");
			}
			if (partSize > int.MaxValue)
			{
				throw new ArgumentException($"The given part size ({partSize}) is too large as it does not fit in a 32 bit int");
			}
			_partSize = (int)partSize;
			return this;
		}

		/// <summary>
		/// Sets whether a data integrity check should be performed during and after upload.
		/// <para>
		/// By default this is disabled.
		/// </para>
		/// <para>
		/// The integrity check consists of two steps. First, each uploaded part
		/// is verified by setting the <b>Content-MD5</b>
		/// header for Amazon S3 to check against its own hash. If they don't match, the AWS SDK
		/// will throw an exception. The header value is the
		/// base64-encoded 128-bit MD5 digest of the part body.
		/// </para>
		/// <para>
		/// The second step is to ensure integrity of the final object merged from the uploaded parts.
		/// This is achieved by comparing the expected ETag value with the actual ETag returned by S3.
		/// However, the ETag value is not a MD5 hash. When S3 combines the parts of a multipart upload
		/// into the final object, the ETag value is set to the hex-encoded MD5 hash of the concatenated
		/// binary-encoded MD5 hashes of each part followed by "-" and the number of parts, for instance:
		/// <pre>57f456164b0e5f365aaf9bb549731f32-95</pre>
		/// <b>Note that AWS doesn't document this, so their hashing algorithm might change without
		/// notice which would lead to false alarm exceptions.
		/// </b>
		/// If the ETags don't match, an <seealso cref="IntegrityCheckException"/> will be thrown after completing
		/// the upload. This will not abort or revert the upload.
		/// 
		/// </para>
		/// </summary>
		/// <param name="checkIntegrity"> <code>true</code> if data integrity should be checked </param>
		/// <returns> this {@code StreamTransferManager} for chaining. </returns>
		/// <exception cref="IllegalStateException"> if <seealso cref="StreamTransferManager.getMultiPartOutputStreams"/> has already
		///                               been called, initiating the upload. </exception>
		public virtual StreamTransferManager CheckIntegrity(bool checkIntegrity)
		{
			EnsureCanSet();
			_checkIntegrity = checkIntegrity;
			return this;
		}

        private void EnsureCanSet()
		{
			if (_queue != null)
			{
				Abort();
				throw new InvalidOperationException("Setters cannot be called after getMultiPartOutputStreams");
			}

		}

		/// <summary>
		/// Get the list of output streams to write to.
		/// <para>
		/// The first call to this method initiates the multipart upload.
		/// All setter methods must be called before this.
		/// </para>
		/// </summary>
		public async Task<IList<MultiPartOutputStream>> GetMultiPartOutputStreams()
		{
			if (_multiPartOutputStreams != null)
			{
				return _multiPartOutputStreams;
			}

			_queue = new BlockingCollection<StreamPart>(_queueCapacity);
			_log.DebugFormat("Initiating multipart upload to {0}/{1}", _bucketName, _putKey);
			var initRequest = new InitiateMultipartUploadRequest { BucketName = _bucketName, Key = _putKey };
			CustomiseInitiateRequest(initRequest);
			InitiateMultipartUploadResponse initResponse = await _s3Client.InitiateMultipartUploadAsync(initRequest);
			_uploadId = initResponse.UploadId;
			_log.InfoFormat("Initiated multipart upload to {0}/{1} with full ID {2}", _bucketName, _putKey, _uploadId);
			try
			{
				_multiPartOutputStreams = new List<MultiPartOutputStream>();

				int partNumberStart = 1;

				for (int i = 0; i < _numStreams; i++)
				{
					int partNumberEnd = (i + 1) * MAX_PART_NUMBER / _numStreams + 1;
					var multiPartOutputStream = new MultiPartOutputStream(partNumberStart, partNumberEnd, _partSize, _queue);
					partNumberStart = partNumberEnd;
					_multiPartOutputStreams.Add(multiPartOutputStream);
				}

				for (int i = 0; i < _numUploadThreads; i++)
				{
					_uploadTasks.Add(Task.Run(UploadTask, _cancellationTokenSource.Token));
				}
			}
			catch (Exception e)
			{
				throw Abort(e);
			}

			return _multiPartOutputStreams;
		}

		/// <summary>
		/// Blocks while waiting for the threads uploading the contents of the streams returned
		/// by <seealso cref="StreamTransferManager.getMultiPartOutputStreams()"/> to finish, then sends a request to S3 to complete
		/// the upload. For the former to complete, it's essential that every stream is closed, otherwise the upload
		/// threads will block forever waiting for more data.
		/// </summary>
		public async Task Complete()
		{
			try
			{
				_log.DebugFormat("{0}: Waiting for pool termination", this);
				await Task.WhenAll(_uploadTasks);
				_log.DebugFormat("{0}: Pool terminated", this);
				if (_leftoverStreamPart != null)
				{
					_log.InfoFormat("{0}: Uploading leftover stream {1}", this, _leftoverStreamPart);
					await UploadStreamPart(_leftoverStreamPart);
					_log.DebugFormat("{0}: Leftover uploaded", this);
				}
				_log.DebugFormat("{0}: Completing", this);
				if (_partETags.Count == 0)
				{
					_log.DebugFormat("{0}: Aborting upload of empty stream", this);
					Abort();
					_log.InfoFormat("{0}: Putting empty object", this);
					var emptyStream = new MemoryStream();
					var request = new PutObjectRequest { 
						BucketName = _bucketName,
						Key = _putKey,
						InputStream = emptyStream};
					request.Metadata.Clear();
					CustomisePutEmptyObjectRequest(request);
					await _s3Client.PutObjectAsync(request);
				}
				else
				{
					var sortedParts = new List<PartETag>(_partETags);
					sortedParts.Sort(new PartNumberComparator());
					var completeRequest = new CompleteMultipartUploadRequest {
						BucketName = _bucketName,
						Key = _putKey,
						UploadId = _uploadId,
						PartETags = sortedParts };
					CustomiseCompleteRequest(completeRequest);
					var completeMultipartUploadResult = await _s3Client.CompleteMultipartUploadAsync(completeRequest);
					if (_checkIntegrity)
					{
                        CheckCompleteFileIntegrity(completeMultipartUploadResult.ETag, sortedParts);
					}
				}
				_log.InfoFormat("{0}: Completed", this);
			}
			catch (IntegrityCheckException e)
			{
				// Nothing to abort. Upload has already finished.
				throw e;
			}
			catch (Exception e)
			{
				throw Abort(e);
			}
		}

		private static void CheckCompleteFileIntegrity(string s3ObjectETag, IList<PartETag> sortedParts)
		{
			string expectedETag = ComputeCompleteFileETag(sortedParts);
			if (!expectedETag.Equals(s3ObjectETag))
			{
				throw new IntegrityCheckException(string.Format("File upload completed, but integrity check failed. Expected ETag: {0} but actual is {1}", expectedETag, s3ObjectETag));
			}
		}

		private static string ComputeCompleteFileETag(IList<PartETag> parts)
		{
			// When S3 combines the parts of a multipart upload into the final object, the ETag value is set to the
			// hex-encoded MD5 hash of the concatenated binary-encoded (raw bytes) MD5 hashes of each part followed by
			// "-" and the number of parts.
			var md = MD5.Create();
			foreach (PartETag partETag in parts)
			{
				var block = Encoding.UTF8.GetBytes(partETag.ETag);
				md.TransformBlock(block, 0, block.Length, block, 0);
			}
			md.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
			// Represent byte array as a 32-digit number hexadecimal format followed by "-<partCount>".
			return string.Format("{0:x32}-{1:D}", new BigInteger(md.Hash!), parts.Count);
		}

		/// <summary>
		/// Aborts the upload and rethrows the argument, wrapped in a RuntimeException if necessary.
		/// Write {@code throw abort(e)} to make it clear to the compiler and readers that the code
		/// stops here.
		/// </summary>
		public virtual Exception Abort(Exception t)
		{
			if (!_isAborting)
			{
				_log.ErrorFormat("Aborting {0} due to error: {1}", this, t);
			}
			throw t;
		}

		/// <summary>
		/// Aborts the upload. Repeated calls have no effect.
		/// </summary>
		public void Abort()
		{
			lock (this)
			{
				if (_isAborting)
				{
					return;
				}
				_isAborting = true;
			}
			if (_uploadTasks != null)
			{
				_cancellationTokenSource.Cancel();
			}
			if (_queue != null)
			{
				_queue.Dispose();
				_queue = null;
			}
			if (_uploadId != null)
			{
				_log.DebugFormat("{this}: Aborting", this);
				var abortMultipartUploadRequest = new AbortMultipartUploadRequest {
					BucketName = _bucketName,
					Key = _putKey,
					UploadId = _uploadId };
				Task.Run(() => _s3Client.AbortMultipartUploadAsync(abortMultipartUploadRequest));
				_log.InfoFormat("{0}: Aborted", this);
			}
		}

		private async Task UploadTask()
		{
			try
			{
				while (true)
				{
					StreamPart? part;
					lock (_queue!)
					{
						if (_finishedCount < _multiPartOutputStreams!.Count)
						{
							part = _queue.Take();
							if (part == StreamPart.POISON)
							{
								_finishedCount++;
								continue;
							}
						}
						else
						{
							break;
						}
					}
					if (part.Size < MultiPartOutputStream.S3_MIN_PART_SIZE)
					{
						/*
						Each stream does its best to avoid producing parts smaller than 5 MB, but if a user doesn't
						write that much data there's nothing that can be done. These are considered 'leftover' parts,
						and must be merged with other leftovers to try producing a part bigger than 5 MB which can be
						uploaded without problems. After the threads have completed there may be at most one leftover
						part remaining, which S3 can accept. It is uploaded in the complete() method.
						*/
						_log.DebugFormat("{0}: Received part {1} < 5 MB that needs to be handled as 'leftover'", this, part);
						StreamPart originalPart = part;
						part = null;
						lock (_leftoverStreamPartLock)
						{
							if (_leftoverStreamPart == null)
							{
								_leftoverStreamPart = originalPart;
								_log.DebugFormat("{0}: Created new leftover part {1}", this, _leftoverStreamPart);
							}
							else
							{
								/*
								Try to preserve order within the data by appending the part with the higher number
								to the part with the lower number. This is not meant to produce a perfect solution:
								if the client is producing multiple leftover parts all bets are off on order.
								*/
								if (_leftoverStreamPart.PartNumber > originalPart.PartNumber)
								{
                                    (_leftoverStreamPart, originalPart) = (originalPart, _leftoverStreamPart);
                                }
                                _leftoverStreamPart.OutputStream.Append(originalPart.OutputStream);
								_log.DebugFormat("{0}: Merged with existing leftover part to create {}", this, _leftoverStreamPart);
								if (_leftoverStreamPart.Size >= MultiPartOutputStream.S3_MIN_PART_SIZE)
								{
									_log.DebugFormat("{}: Leftover part can now be uploaded as normal and reset", this);
									part = _leftoverStreamPart;
									_leftoverStreamPart = null;
								}
							}
						}
					}
					if (part != null)
					{
						await UploadStreamPart(part);
					}
				}
			}
			catch (Exception t)
			{
				throw Abort(t);
			}
		}

		private async Task UploadStreamPart(StreamPart part)
		{
			_log.DebugFormat("{}: Uploading {}", this, part);

			UploadPartRequest uploadRequest = new() {
				BucketName = _bucketName,
				Key = _putKey,
				UploadId = _uploadId,
				PartNumber = part.PartNumber,
				InputStream = part.InputStream,
				PartSize = part.Size};
			if (_checkIntegrity)
			{
				uploadRequest.MD5Digest = part.MD5Digest;
			}
			CustomiseUploadPartRequest(uploadRequest);

			var uploadPartResult = await _s3Client.UploadPartAsync(uploadRequest, _cancellationTokenSource.Token);
			PartETag partETag = new(uploadPartResult);
			_partETags.Add(partETag);
			_log.InfoFormat("{0}: Finished uploading {1}", this, part);
		}

		public override string ToString()
			=> $"[Manager uploading to {_bucketName}/{_putKey} with id {Utils.SkipMiddle(_uploadId!, 21)}]";

		// These methods are intended to be overridden for more specific interactions with the AWS API.
		public virtual void CustomiseInitiateRequest(InitiateMultipartUploadRequest request)
		{ }

		public virtual void CustomiseUploadPartRequest(UploadPartRequest request)
		{ }

		public virtual void CustomiseCompleteRequest(CompleteMultipartUploadRequest request)
		{ }

		public virtual void CustomisePutEmptyObjectRequest(PutObjectRequest request)
		{ }

		private class PartNumberComparator : IComparer<PartETag>
		{
			public virtual int Compare(PartETag? o1, PartETag? o2)
			{
				int partNumber1 = o1?.PartNumber ?? 0;
				int partNumber2 = o2?.PartNumber ?? 0;

				return Math.Sign(partNumber1 - partNumber2);
			}
		}
	}
}
