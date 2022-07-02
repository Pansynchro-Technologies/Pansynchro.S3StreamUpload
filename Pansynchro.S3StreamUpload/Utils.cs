using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace Pansynchro.S3StreamUpload
{
	/// <summary>
	/// Miscellaneous useful functions.
	/// </summary>
	internal class Utils
	{
		/// <summary>
		/// Shortens the given string to the given length by replacing the middle with ...,
		/// unless the string is already short enough or almost short enough in which case it is returned unmodified.
		/// </summary>
		public static string SkipMiddle(string str, int length)
		{
			int inputLength = str.Length;
			if (inputLength < length * 1.1)
			{
				return str;
			}
			int sideLength = (length - 3) / 2;
			var builder = new StringBuilder(length);
			builder.Append(str, 0, sideLength);
			builder.Append("...");
			builder.Append(str, inputLength - sideLength, inputLength);
			return builder.ToString();
		}

		public static HashAlgorithm Md5()
		{
			var md = HashAlgorithm.Create("MD5");
			if (md == null) {
				throw new Exception("MD5 algorithm was not found");
			}
			md.Clear();
			return md;
		}
	}
}
