using DbBlobLib.Models;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using System.IO;
using System.Threading;

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
            OnSave(GetNewRecord());
        }

        public override void Delete(DeleteSnapshotsOption deleteSnapshotsOption = DeleteSnapshotsOption.None, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            base.Delete(deleteSnapshotsOption, accessCondition, options, operationContext);            
            OnDelete(GetDeletedRecord());
        }

        public override void UploadFromByteArray(byte[] buffer, int index, int count, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            base.UploadFromByteArray(buffer, index, count, accessCondition, options, operationContext);            
            OnSave(GetNewRecord());
        }

        public override Task UploadFromFileAsync(string path)
        {
            var task = base.UploadFromFileAsync(path);
            task.ContinueWith((t) => OnSave(GetNewRecord()));
            return task;
        }

        public override Task UploadFromByteArrayAsync(byte[] buffer, int index, int count)
        {
            var task = base.UploadFromByteArrayAsync(buffer, index, count);
            task.ContinueWith((t) => OnSave(GetNewRecord()));
            return task;
        }
        
        public override Task UploadFromStreamAsync(Stream source)
        {
            var task = base.UploadFromStreamAsync(source);
            task.ContinueWith((t) => OnSave(GetNewRecord()));
            return task;
        }

        public override Task UploadFromByteArrayAsync(byte[] buffer, int index, int count, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            var task = base.UploadFromByteArrayAsync(buffer, index, count, accessCondition, options, operationContext);
            task.ContinueWith((t) => OnSave(GetNewRecord()));
            return task;
        }

        public override Task DeleteAsync()
        {
            var task = base.DeleteAsync();
            task.ContinueWith((t) => OnDelete(GetDeletedRecord()));
            return task;
        }

        public override Task DeleteAsync(DeleteSnapshotsOption deleteSnapshotsOption, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            var task = base.DeleteAsync(deleteSnapshotsOption, accessCondition, options, operationContext, cancellationToken);
            task.ContinueWith((t) => OnDelete(GetDeletedRecord()));
            return task;
        }

        public override Task DeleteAsync(CancellationToken cancellationToken)
        {
            var task = base.DeleteAsync(cancellationToken);
            task.ContinueWith((t) => OnDelete(GetDeletedRecord()));
            return task;
        }

        #region inner helper methods
        private T GetNewRecord()
        {
            T result = new T();
            result.Container = this.Container.Name;
            result.Name = this.Name;
            result.LastModified = this.Properties?.LastModified?.DateTime;
            result.Length = this.Properties?.Length ?? 0;
            return result;
        }

        private T GetDeletedRecord()
        {
            T result = FindRecord();
            if (result == null) return default(T);
            result.LastDeleted = DateTime.UtcNow;
            return result;
        }
        #endregion
    }
}
