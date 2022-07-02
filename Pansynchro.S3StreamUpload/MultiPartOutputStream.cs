using log4net;
using System;
using System.Collections.Concurrent;
using System.IO;

namespace Pansynchro.S3StreamUpload
{
	/// <summary>
	/// An {@code OutputStream} which packages data written to it into discrete <seealso cref="StreamPart"/>s which can be obtained
	/// in a separate thread via iteration and uploaded to S3.
	/// <para>
	/// A single {@code MultiPartOutputStream} is allocated a range of part numbers it can assign to the {@code StreamPart}s
	/// it produces, which is determined at construction.
	/// </para>
	/// <para>
	/// It's essential to call
	/// <seealso cref="MultiPartOutputStream.close()"/> when finished so that it can create the final {@code StreamPart} and consumers
	/// can finish.
	/// </para>
	/// <para>
	/// Writing to the stream may lead to trying to place a completed part on a queue,
	/// which will block if the queue is full and may lead to an {@code InterruptedException}.
	/// </para>
	/// </summary>
	public class MultiPartOutputStream : Stream
	{
		private const int MB = 1024 * 1024;

		private static readonly ILog log = LogManager.GetLogger(typeof(MultiPartOutputStream));

		private ConvertibleOutputStream? _currentStream;

		public static readonly int S3_MIN_PART_SIZE = 5 * MB;
		private static readonly int STREAM_EXTRA_ROOM = MB;

		private readonly BlockingCollection<StreamPart> queue;

		private readonly int partNumberStart;
		private readonly int partNumberEnd;
		private readonly int partSize;
		private int currentPartNumber;

		/// <summary>
		/// Creates a new stream that will produce parts of the given size with part numbers in the given range.
		/// </summary>
		/// <param name="partNumberStart"> the part number of the first part the stream will produce. Minimum 1. </param>
		/// <param name="partNumberEnd">   1 more than the last part number that the parts are allowed to have. Maximum 10 001. </param>
		/// <param name="partSize">        the minimum size in bytes of parts to be produced. </param>
		/// <param name="queue">           where stream parts are put on production. </param>
		internal MultiPartOutputStream(int partNumberStart, int partNumberEnd, int partSize, BlockingCollection<StreamPart> queue)
		{
			if (partNumberStart < 1)
			{
				throw new IndexOutOfRangeException("The lowest allowed part number is 1. The value given was " + partNumberStart);
			}
			if (partNumberEnd > 10001)
			{
				throw new IndexOutOfRangeException("The highest allowed part number is 10 000, so partNumberEnd must be at most 10 001. The value given was " + partNumberEnd);
			}
			if (partNumberEnd <= partNumberStart)
			{
				throw new IndexOutOfRangeException($"The part number end ({partNumberEnd}) must be greater than the part number start ({partNumberStart}).");
			}
			if (partSize < S3_MIN_PART_SIZE)
			{
				throw new ArgumentException($"The given part size ({partSize}) is less than 5 MB.");
			}

			this.partNumberStart = partNumberStart;
			this.partNumberEnd = partNumberEnd;
			this.queue = queue;
			this.partSize = partSize;

			log.DebugFormat("Creating {0}", this);

			currentPartNumber = partNumberStart;
			_currentStream = new ConvertibleOutputStream(StreamAllocatedSize);
		}

		/// <summary>
		/// Returns the initial capacity in bytes of the {@code ByteArrayOutputStream} that a part uses.
		/// This consists of the size that the user asks for, the extra 5 MB to avoid small parts(see the comment in
		/// checkSize()), and some extra space to make resizing and copying unlikely.
		/// </summary>
		private int StreamAllocatedSize => partSize + S3_MIN_PART_SIZE + STREAM_EXTRA_ROOM;

		/// <summary>
		/// Checks if the stream currently contains enough data to create a new part.
		/// </summary>
		private void CheckSize()
		{
			/*
			This class avoids producing parts < 5 MB if possible by only producing a part when it has an extra 5 MB to spare
			for the next part. For example, suppose the following. A stream is producing parts of 10 MB. Someone writes
			10 MB and then calls this method, and then writes only 3 MB more before closing. If the initial 10 MB were
			immediately packaged into a StreamPart and a new ConvertibleOutputStream was started on for the rest, it would
			end up producing a part with just 3 MB. So instead the class waits until it contains 15 MB of data and then it
			splits the stream into two: one with 10 MB that gets produced as a part, and one with 5 MB that it continues with.
			In this way users of the class are less likely to encounter parts < 5 MB which cause trouble: see the caveat
			on order in the StreamTransferManager and the way it handles these small parts, referred to as 'leftover'.
			Such parts are only produced when the user closes a stream that never had more than 5 MB written to it.
			 */
			if (_currentStream == null)
			{
				throw new InvalidOperationException("The stream is closed and cannot be written to.");
			}
			if (_currentStream.Length > partSize + S3_MIN_PART_SIZE)
			{
				ConvertibleOutputStream newStream = _currentStream.Split((int)_currentStream!.Length - S3_MIN_PART_SIZE, StreamAllocatedSize);
				PutCurrentStream();
				_currentStream = newStream;
			}
		}

		private void PutCurrentStream()
		{
			if (_currentStream == null || _currentStream.Length == 0)
			{
				return;
			}
			if (currentPartNumber >= partNumberEnd)
			{
				throw new IndexOutOfRangeException(string.Format("This stream was allocated the part numbers from {0:D} (inclusive) to {1:D} (exclusive)" + "and it has gone beyond the end.", partNumberStart, partNumberEnd));
			}
			var streamPart = new StreamPart(_currentStream, currentPartNumber++);
			log.DebugFormat("Putting {0} on queue", streamPart);
			queue.Add(streamPart);
		}

		private readonly byte[] _byteBuffer = new byte[1];

		public void Write(byte b)
		{
			_byteBuffer[0] = b;
			Write(_byteBuffer);
		}

		public override void Write(byte[] b, int off, int len)
		{
			if (_currentStream == null)
			{
				log.ErrorFormat("Trying to write to {0} when it's already closed", this);
				throw new InvalidOperationException();
			}

			_currentStream.Write(b, off, len);
			CheckSize();
		}

		public void Write(byte[] b)
		{
			Write(b, 0, b.Length);
		}

		/// <summary>
		/// Packages any remaining data into a <seealso cref="StreamPart"/> and signals to the {@code StreamTransferManager} that there are no more parts
		/// afterwards. You cannot write to the stream after it has been closed.
		/// </summary>
		public override void Close()
		{
			log.InfoFormat("Called close() on {0}", this);
			if (_currentStream == null)
			{
				log.WarnFormat("{0} is already closed", this);
				return;
			}
			PutCurrentStream();
			log.DebugFormat("Placing poison pill on queue for {0}", this);
			queue.Add(StreamPart.POISON);
			_currentStream = null;
		}

		public override string ToString()
		{
			return $"[MultipartOutputStream for parts {partNumberStart} - {partNumberEnd - 1}]";
		}

		public override bool CanRead => true;

		public override bool CanSeek => false;

		public override bool CanWrite => true;

		public override long Length => _currentStream!.Length;

		public override long Position { 
			get => _currentStream!.Position;
			set => throw new InvalidOperationException();
		}

		public override void Flush()
		{
			_currentStream!.Flush();
		}

		public override int Read(byte[] buffer, int offset, int count)
			=> throw new InvalidOperationException();

		public override long Seek(long offset, SeekOrigin origin)
			=> throw new InvalidOperationException();

		public override void SetLength(long value)
			=> throw new InvalidOperationException();
	}
}
