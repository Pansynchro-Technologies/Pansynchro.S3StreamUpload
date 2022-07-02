using System;
using System.IO;
using System.Security.Cryptography;
using log4net;

namespace Pansynchro.S3StreamUpload
{
	/// <summary>
	/// A ByteArrayOutputStream with some useful additional functionality.
	/// </summary>
	internal class ConvertibleOutputStream : MemoryStream
	{

		private static readonly ILog log = LogManager.GetLogger(typeof(ConvertibleOutputStream));

		public ConvertibleOutputStream(int initialCapacity) : base(initialCapacity)
		{
		}

		/// <summary>
		/// Creates an InputStream sharing the same underlying byte array, reducing memory usage and copying time.
		/// </summary>
		public virtual Stream ToInputStream()
		{
			return new MemoryStream(GetBuffer(), false);
		}

		/// <summary>
		/// Truncates this stream to a given size and returns a new stream containing a copy of the remaining data.
		/// </summary>
		/// <param name="countToKeep">                 number of bytes to keep in this stream, starting from the first written byte. </param>
		/// <param name="initialCapacityForNewStream"> buffer capacity to construct the new stream (NOT the number of bytes
		///                                    that the new stream will take from this one) </param>
		/// <returns> a new stream containing all the bytes previously contained in this one, i.e. from countToKeep + 1 onwards. </returns>
		public virtual ConvertibleOutputStream Split(int countToKeep, int initialCapacityForNewStream)
		{
			var newCount = (int)Length - countToKeep;
			log.DebugFormat("Splitting stream of size {0} into parts with sizes {1} and {2}", Length, countToKeep, newCount);
			initialCapacityForNewStream = Math.Max(initialCapacityForNewStream, newCount);
			var newStream = new ConvertibleOutputStream(initialCapacityForNewStream);
			newStream.Write(GetBuffer(), countToKeep, newCount);
			SetLength(countToKeep);
			return newStream;
		}

		/// <summary>
		/// Concatenates the given stream to this stream.
		/// </summary>
		public virtual void Append(ConvertibleOutputStream otherStream)
		{
			otherStream.WriteTo(this);
		}

		public virtual byte[] MD5Digest => MD5.HashData(GetBuffer().AsSpan(0, (int)Length));
	}
}
