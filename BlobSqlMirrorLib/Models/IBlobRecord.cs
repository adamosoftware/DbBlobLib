using System;

namespace DbBlobLib.Models
{
    public interface IBlobRecord
    {
        string Container { get; set; }
        string Name { get; set; }
        DateTime? LastModified { get; set; }
        DateTime? LastDeleted { get; set; }
        long Length { get; set; }
        bool IsSnapshot { get; set; }
    }
}
