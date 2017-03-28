using DbBlobLib.Models;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;

namespace DbBlobLib
{
    public abstract class CloudBlockDbBlob<T> : CloudBlockBlob where T : IBlobRecord, new()
    {
        public CloudBlockDbBlob(Uri blobAbsoluteUri) : base(blobAbsoluteUri)
        {
        }

        public CloudBlockDbBlob(Uri blobAbsoluteUri, StorageCredentials credentials) : base(blobAbsoluteUri, credentials)
        {               
        }

        public CloudBlockDbBlob(Uri blobAbsoluteUri, DateTimeOffset? snapshotTime, StorageCredentials credentials) : base(blobAbsoluteUri, snapshotTime, credentials)
        {
        }

        protected abstract void OnSave(T record);
        protected abstract T FindRecord();
        protected abstract void OnDelete(T record);

        public override void UploadFromFile(string path, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {            
            base.UploadFromFile(path, accessCondition, options, operationContext);            
            OnSave(new T());
        }

        public override void Delete(DeleteSnapshotsOption deleteSnapshotsOption = DeleteSnapshotsOption.None, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            base.Delete(deleteSnapshotsOption, accessCondition, options, operationContext);            
            OnDelete(FindRecord());
        }

        public override void UploadFromByteArray(byte[] buffer, int index, int count, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            base.UploadFromByteArray(buffer, index, count, accessCondition, options, operationContext);            
            OnSave(new T());
        }

        public override Task UploadFromFileAsync(string path)
        {
            var task = base.UploadFromFileAsync(path);
            task.ContinueWith((t) => OnSave(new T()));
            return task;
        }
    }
}
