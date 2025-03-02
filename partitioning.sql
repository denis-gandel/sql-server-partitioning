CREATE DATABASE PartitioningDB;

USE PartitioningDB;

CREATE TABLE Users
(
    Id UNIQUEIDENTIFIER DEFAULT NEWID() PRIMARY KEY,
    Firstname VARCHAR(64) NOT NULL,
    Lastname VARCHAR(64) NOT NULL,
    BirthDate DATE NOT NULL DEFAULT GETDATE(),
    Country VARCHAR(100) NOT NULL
);

SELECT *
FROM Users;

/*
PARTITIONS: Define the LOGIC on how to divide your data into partitions!
Based on Partition Key like (Region, Dates, and others)
*/

-- CREATE PARTITIONS
CREATE PARTITION FUNCTION PartitionByBirthDate (DATE)
AS RANGE LEFT FOR VALUES ('2023-12-31', '2024-12-31', '2025-12-31');

-- SELECT PARTITIONS CREATED

SELECT *
FROM sys.partition_functions;

-- CREATE FILEGROUPS
/*
 FILEGROUP: Is the logical CONTAINER of one or more data files to help organize partitions.
 */

ALTER DATABASE PartitioningDB ADD FILEGROUP FG_2023;
ALTER DATABASE PartitioningDB ADD FILEGROUP FG_2024;
ALTER DATABASE PartitioningDB ADD FILEGROUP FG_2025;
ALTER DATABASE PartitioningDB ADD FILEGROUP FG_2026;

-- SELECT FILEGROUPS
/*
By default exist PRIMARY filegroup that is where all objects of database is stored
*/

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

SELECT *
FROM sys.database_files;

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

CREATE TABLE dbo.BirthDate_Partitioned
(
    BirthDateID INT,
    BirthDate DATE,
    Users INT
) ON SchemePartitionByBirthDate (BirthDate);

INSERT INTO dbo.BirthDate_Partitioned VALUES (1, '2023-12-01', 100);

INSERT INTO dbo.BirthDate_Partitioned (BirthDateID, BirthDate, Users) VALUES
(1, '2023-01-15', 101),
(2, '2023-02-20', 202),
(3, '2023-03-10', 303),
(4, '2023-04-05', 404),
(5, '2023-05-21', 105),
(6, '2023-06-14', 206),
(7, '2023-07-30', 307),
(8, '2023-08-19', 408),
(9, '2023-09-25', 109),
(10, '2023-10-01', 210),
(11, '2023-11-12', 311),
(12, '2023-12-23', 412),
(13, '2024-01-08', 113),
(14, '2024-02-17', 214),
(15, '2024-03-29', 315),
(16, '2024-04-04', 416),
(17, '2024-05-11', 117),
(18, '2024-06-20', 218),
(19, '2024-07-09', 319),
(20, '2024-08-14', 420),
(21, '2024-09-27', 121),
(22, '2024-10-05', 222),
(23, '2024-11-13', 323),
(24, '2024-12-07', 424),
(25, '2025-01-18', 125),
(26, '2025-02-28', 226),
(27, '2025-03-22', 327),
(28, '2025-04-15', 428),
(29, '2025-05-02', 129),
(30, '2025-06-18', 230),
(31, '2025-07-09', 331),
(32, '2025-08-05', 432),
(33, '2025-09-27', 133),
(34, '2025-10-21', 234),
(35, '2025-11-16', 335),
(36, '2025-12-08', 436),
(37, '2026-01-19', 137),
(38, '2026-02-28', 238),
(39, '2026-03-30', 339),
(40, '2026-04-12', 440),
(41, '2026-05-03', 141),
(42, '2026-06-08', 242),
(43, '2026-07-26', 343),
(44, '2026-08-14', 444),
(45, '2026-09-05', 145),
(46, '2026-10-23', 246),
(47, '2026-11-29', 347),
(48, '2026-12-18', 448),
(49, '2023-03-12', 149),
(50, '2023-06-25', 250),
(51, '2023-09-07', 351),
(52, '2023-12-14', 452),
(53, '2024-01-29', 153),
(54, '2024-04-10', 254),
(55, '2024-07-08', 355),
(56, '2024-10-28', 456),
(57, '2025-01-06', 157),
(58, '2025-04-30', 258),
(59, '2025-07-16', 359),
(60, '2025-10-03', 460),
(61, '2026-01-12', 161),
(62, '2026-04-19', 262),
(63, '2026-07-01', 363),
(64, '2026-10-07', 464),
(65, '2023-02-17', 165),
(66, '2023-05-24', 266),
(67, '2023-08-14', 367),
(68, '2023-11-01', 468),
(69, '2024-02-29', 169),
(70, '2024-06-11', 270),
(71, '2024-09-04', 371),
(72, '2024-12-20', 472),
(73, '2025-03-09', 173),
(74, '2025-06-15', 274),
(75, '2025-09-28', 375),
(76, '2025-12-06', 476),
(77, '2026-02-04', 177),
(78, '2026-05-17', 278),
(79, '2026-08-22', 379),
(80, '2026-11-30', 480),
(81, '2023-01-09', 181),
(82, '2023-04-10', 282),
(83, '2023-07-19', 383),
(84, '2023-10-02', 484),
(85, '2024-01-29', 185),
(86, '2024-04-18', 286),
(87, '2024-07-27', 387),
(88, '2024-10-14', 488),
(89, '2025-02-05', 189),
(90, '2025-05-23', 290),
(91, '2025-08-08', 391),
(92, '2025-11-06', 492),
(93, '2026-03-10', 193),
(94, '2026-06-30', 294),
(95, '2026-09-27', 395),
(96, '2026-12-22', 496),
(97, '2023-02-15', 197),
(98, '2024-06-04', 298),
(99, '2025-10-09', 399),
(100, '2026-12-06', 500);

SELECT * FROM dbo.BirthDate_Partitioned;

SELECT 
    p.partition_number AS PartitionNumber,
    f.name AS PartitionFilegroup,
    p.rows AS NumberOfRows
FROM sys.partitions p
JOIN sys.destination_data_spaces dds ON p.partition_number = dds.destination_id
JOIN sys.filegroups f ON dds.data_space_id = f.data_space_id
WHERE OBJECT_NAME(p.object_id) = 'BirthDate_Partitioned';


-- ARREGLAR ESTO PARA QUE EN VEZ DE USAR USUARIOS USAR VENTAS PARA QUE SEA MAS ENTENDIBLE LO DE USUARIOS

-- .ndf Non-Primary Data File

SELECT * 
FROM dbo.BirthDate_Partitioned
WHERE YEAR(BirthDate) = 2023;

SELECT * 
FROM dbo.BirthDate_Partitioned
WHERE YEAR(BirthDate) = 2024;

SELECT * 
FROM dbo.BirthDate_Partitioned
WHERE YEAR(BirthDate) = 2025;

SELECT * 
FROM dbo.BirthDate_Partitioned
WHERE YEAR(BirthDate) = 2026;
