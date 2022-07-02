using System;

namespace Pansynchro.S3StreamUpload
{
    /**
     * Thrown when final integrity check fails. It suggests that the multipart upload failed
     * due to data corruption. See {@link StreamTransferManager#checkIntegrity(boolean)} for details.
     */
    public class IntegrityCheckException : Exception
    {
        public IntegrityCheckException(string message) : base(message) { }
    }
}
