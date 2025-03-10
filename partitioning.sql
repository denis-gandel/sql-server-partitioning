CREATE DATABASE PartitioningDB;

USE PartitioningDB;

-- CREATE PARTITIONS
CREATE PARTITION FUNCTION PartitionByBirthDate (DATE)
AS RANGE LEFT FOR VALUES ('2023-12-31', '2024-12-31', '2025-12-31');

-- SELECT PARTITIONS CREATED

SELECT *
FROM sys.partition_functions;

-- CREATE FILEGROUPS

ALTER DATABASE PartitioningDB ADD FILEGROUP FG_2023;
ALTER DATABASE PartitioningDB ADD FILEGROUP FG_2024;
ALTER DATABASE PartitioningDB ADD FILEGROUP FG_2025;
ALTER DATABASE PartitioningDB ADD FILEGROUP FG_2026;

-- SELECT FILEGROUPS

SELECT *
FROM sys.filegroups
WHERE type = 'FG';

-- CREATE DATA FILES
ALTER DATABASE PartitioningDB ADD FILE
(
    NAME = P_2023, -- LOGICAL NAME
    FILENAME = '/var/opt/mssql/data/P_2023.ndf'
) TO FILEGROUP FG_2023;

ALTER DATABASE PartitioningDB ADD FILE
(
    NAME = P_2024, -- LOGICAL NAME
    FILENAME = '/var/opt/mssql/data/P_2024.ndf'
) TO FILEGROUP FG_2024;

ALTER DATABASE PartitioningDB ADD FILE
(
    NAME = P_2025, -- LOGICAL NAME
    FILENAME = '/var/opt/mssql/data/P_2025.ndf'
) TO FILEGROUP FG_2025;

ALTER DATABASE PartitioningDB ADD FILE
(
    NAME = P_2026, -- LOGICAL NAME
    FILENAME = '/var/opt/mssql/data/P_2026.ndf'
) TO FILEGROUP FG_2026;

SELECT
    fg.name AS FilegroupName,
    mf.name AS LogicalFileName,
    mf.physical_name AS PhysicalFilePath,
    mf.size / 128 AS SizeInMB
FROM sys.filegroups fg
    JOIN
    sys.master_files mf ON fg.data_space_id = mf.data_space_id
WHERE mf.database_id = DB_ID('PartitioningDB');

-- CREATE PARTITION SCHEMA

CREATE PARTITION SCHEME SchemePartitionByBirthDate 
AS PARTITION PartitionByBirthDate 
TO (FG_2023, FG_2024, FG_2025, FG_2026);

SELECT
    ps.name AS PartitionSchemeName,
    pf.name AS PartitionFunctionName,
    ds.destination_id AS PartitionID,
    fg.name AS FilegroupName
FROM sys.partition_schemes ps
    JOIN
    sys.partition_functions pf ON ps.function_id = pf.function_id
    JOIN
    sys.destination_data_spaces ds ON ps.data_space_id = ds.partition_scheme_id
    JOIN
    sys.filegroups fg ON ds.data_space_id = fg.data_space_id;

-- CREATE PARITIONED TABLE

CREATE TABLE dbo.UsersPartitioning_ByBirthDate
(
    Id UNIQUEIDENTIFIER DEFAULT NEWID(),
    Firstname VARCHAR(64) NOT NULL,
    Lastname VARCHAR(64) NOT NULL,
    BirthDate DATE NOT NULL DEFAULT GETDATE(),
    Country VARCHAR(100) NOT NULL
) ON SchemePartitionByBirthDate (BirthDate);

-- TEST SQL QUERIES

SELECT * FROM dbo.UsersPartitioning_ByBirthDate;

SELECT 
    p.partition_number AS PartitionNumber,
    f.name AS PartitionFilegroup,
    p.rows AS NumberOfRows
FROM sys.partitions p
JOIN sys.destination_data_spaces dds ON p.partition_number = dds.destination_id
JOIN sys.filegroups f ON dds.data_space_id = f.data_space_id
WHERE OBJECT_NAME(p.object_id) = 'UsersPartitioning_ByBirthDate';

SELECT * 
FROM dbo.UsersPartitioning_ByBirthDate
WHERE YEAR(BirthDate) = 2023;

SELECT * 
FROM dbo.UsersPartitioning_ByBirthDate
WHERE YEAR(BirthDate) = 2024;

SELECT * 
FROM dbo.UsersPartitioning_ByBirthDate
WHERE YEAR(BirthDate) = 2025;

SELECT * 
FROM dbo.UsersPartitioning_ByBirthDate
WHERE YEAR(BirthDate) = 2026;

SELECT * 
FROM dbo.UsersPartitioning_ByBirthDate 
WHERE Id = '8c00f688-a25f-4328-8951-092b1e5ee3d2';
