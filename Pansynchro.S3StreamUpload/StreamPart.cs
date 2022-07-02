using System;
using System.Buffers.Text;
using System.IO;

namespace Pansynchro.S3StreamUpload
{
	/// <summary>
	/// A simple class which holds some data which can be uploaded to S3 as part of a multipart upload and a part number
	/// identifying it.
	/// </summary>
	internal class StreamPart
	{

		private readonly ConvertibleOutputStream _stream;
		private readonly int _partNumber;

		/// <summary>
		/// A 'poison pill' placed on the queue to indicate that there are no further parts from a stream.
		/// </summary>
		internal static readonly StreamPart POISON = new (null!, -1);

		public StreamPart(ConvertibleOutputStream stream, int partNumber)
		{
			this._stream = stream;
			this._partNumber = partNumber;
		}

		public virtual int PartNumber => _partNumber;

		public virtual ConvertibleOutputStream OutputStream => _stream;

		public virtual Stream InputStream => _stream.ToInputStream();

		public virtual long Size => _stream.Length;

		public virtual string MD5Digest => Convert.ToBase64String(_stream.MD5Digest);

		public override string ToString()
		{
			return string.Format("[Part number {0:D} {1}]", _partNumber, _stream == null ? "with null stream" : string.Format("containing {0:F2} MB", Size / (1024 * 1024.0)));
		}
	}
}
