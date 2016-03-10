-- This is just a helper SQL script used to aid development.

DROP TABLE dbo.Events;
DROP TABLE dbo.Streams;
DROP TYPE dbo.NewStreamEvents;
DROP PROC dbo.CreateStream;
DROP PROC dbo.AppendStream;

CREATE TABLE dbo.Streams(
    Id                  CHAR(40)                                NOT NULL,
    IdOriginal          NVARCHAR(1000)                          NOT NULL,
    IdInternal          INT                 IDENTITY(1,1)       NOT NULL,
    IsDeleted           BIT                 DEFAULT (0)         NOT NULL,
    CONSTRAINT PK_Streams PRIMARY KEY CLUSTERED (IdInternal)
);
CREATE UNIQUE NONCLUSTERED INDEX IX_Streams_Id ON dbo.Streams (Id);
 
CREATE TABLE dbo.Events(
    StreamIdInternal    INT                                     NOT NULL,
    StreamVersion       INT                                     NOT NULL,
    Ordinal             BIGINT                 IDENTITY(0,1)    NOT NULL,
    Id                  UNIQUEIDENTIFIER                        NOT NULL,
    Created             DATETIME                                NOT NULL,
    [Type]              NVARCHAR(128)                           NOT NULL,
    JsonData            VARBINARY(5000)                         NOT NULL,
    JsonMetadata        VARBINARY(2000)                                 ,
    CONSTRAINT PK_Events PRIMARY KEY CLUSTERED (Ordinal),
    CONSTRAINT FK_Events_Streams FOREIGN KEY (StreamIdInternal) REFERENCES dbo.Streams(IdInternal)
);

CREATE UNIQUE NONCLUSTERED INDEX IX_Events_StreamIdInternal_Revision ON dbo.Events (StreamIdInternal, StreamVersion);

CREATE TYPE dbo.NewStreamEvents AS TABLE (
    StreamVersion       INT IDENTITY(0,1)                       NOT NULL,
    Id                  UNIQUEIDENTIFIER                        NOT NULL,
    Created             DATETIME            DEFAULT(GETDATE())  NOT NULL,
    [Type]              NVARCHAR(128)                           NOT NULL,
    JsonData            VARBINARY(5000)                         NULL    ,
    JsonMetadata        VARBINARY(2000)                         NULL
);

GO
 
-- Create Stream (Append with expected version = no version)
CREATE PROC dbo.CreateStream(@streamId NVARCHAR(40))
AS
BEGIN
    DECLARE @newEvents dbo.NewStreamEvents;
    INSERT INTO @newEvents
    (
        Id              ,
        [Type]          ,
        JsonData        ,
        JsonMetadata
    ) VALUES
    ('00000000-0000-0000-0000-000000000001', 'type1', CONVERT(VARBINARY(5000), '\"data1\"'), CONVERT(VARBINARY(2000),'\"meta1\"')),
    ('00000000-0000-0000-0000-000000000002', 'type2', CONVERT(VARBINARY(5000), '\"data2\"'), CONVERT(VARBINARY(2000),'\"meta2\"')),
    ('00000000-0000-0000-0000-000000000003', 'type3', CONVERT(VARBINARY(5000), '\"data3\"'), CONVERT(VARBINARY(2000),'\"meta3\"')),
    ('00000000-0000-0000-0000-000000000004', 'type4', CONVERT(VARBINARY(5000), '\"data4\"'), CONVERT(VARBINARY(2000),'\"meta4\"'));

    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
    BEGIN TRANSACTION CreateStream;
        DECLARE @count AS INT;
        DECLARE @streamIdInternal AS INT;
        BEGIN
            INSERT INTO dbo.Streams (Id, IdOriginal) VALUES (@streamId, @streamId);
            SELECT @streamIdInternal = SCOPE_IDENTITY();

            INSERT INTO dbo.Events (StreamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata)
                 SELECT @streamIdInternal,
                        StreamVersion,
                        Id,
                        Created,
                        [Type],
                        JsonData,
                        JsonMetadata
                   FROM @newEvents;
 
        END;
    COMMIT TRANSACTION CreateStream;
END
GO

-- Create Stream (Append with expected version = no version)
CREATE PROC dbo.AppendStream(
    @streamId NVARCHAR(40),
    @expectedStreamVersion INT
)
AS
BEGIN
    DECLARE @newEvents dbo.NewStreamEvents;
    INSERT INTO @newEvents
    (
        Id              ,
        [Type]          ,
        JsonData        ,
        JsonMetadata
    ) VALUES
    ('00000000-0000-0000-0000-000000000005', 'type1', CONVERT(VARBINARY(5000), '\"data1\"'), CONVERT(VARBINARY(2000),'\"meta1\"')),
    ('00000000-0000-0000-0000-000000000006', 'type1', CONVERT(VARBINARY(5000), '\"data2\"'), CONVERT(VARBINARY(2000),'\"meta2\"')),
    ('00000000-0000-0000-0000-000000000007', 'type1', CONVERT(VARBINARY(5000), '\"data3\"'), CONVERT(VARBINARY(2000),'\"meta3\"')),
    ('00000000-0000-0000-0000-000000000008', 'type1', CONVERT(VARBINARY(5000), '\"data4\"'), CONVERT(VARBINARY(2000),'\"meta4\"'));

    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
    BEGIN TRANSACTION AppendStream;

        DECLARE @streamIdInternal AS INT;
        DECLARE @latestStreamVersion  AS INT;

         SELECT @streamIdInternal = Streams.IdInternal
           FROM Streams
          WHERE Streams.Id = @streamId;

          IF @streamIdInternal IS NULL
          BEGIN
             ROLLBACK TRANSACTION AppendStream;
             RAISERROR('WrongExpectedVersion', 16, 1);
             RETURN;
          END

         SELECT TOP(1)
                @latestStreamVersion = Events.StreamVersion
           FROM Events
          WHERE Events.StreamIDInternal = @streamIdInternal
       ORDER BY Events.Ordinal DESC;

         IF @latestStreamVersion != @expectedStreamVersion
         BEGIN
            ROLLBACK TRANSACTION AppendStream;
            RAISERROR('WrongExpectedVersion', 16, 2);
            RETURN;
         END

    INSERT INTO dbo.Events (StreamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata)
            SELECT @streamIdInternal,
                StreamVersion + @latestStreamVersion + 1,
                Id,
                Created,
                [Type],
                JsonData,
                JsonMetadata
            FROM @newEvents;
 
    COMMIT TRANSACTION AppendStream;
END
GO

EXEC dbo.CreateStream 'stream-1';
EXEC dbo.CreateStream 'stream-2';
EXEC dbo.CreateStream 'stream-3';
EXEC dbo.CreateStream 'stream-4';

/* AppendStream with ExpectedVersion */
EXEC dbo.AppendStream 'stream-4', 4;
EXEC dbo.AppendStream 'stream-4', 3;

GO

SELECT * FROM dbo.Streams;
SELECT * FROM dbo.Events;

DECLARE @ordinal AS INT = 2;
DECLARE @count AS INT = 5;

/* READ ALL FORWARD SQL Server 2008+ */
     SELECT TOP(@count)
            Streams.IdOriginal As StreamId,
            Events.StreamVersion,
            Events.Ordinal,
            Events.Id AS EventId,
            Events.Created,
            Events.Type,
            Events.JsonData,
            Events.JsonMetadata
       FROM Events
 INNER JOIN Streams
         ON Events.StreamIdInternal = Streams.IdInternal
      WHERE Events.Ordinal >= @ordinal
   ORDER BY Events.Ordinal;

/* READ ALL BACKWARD SQL Server 2008+ */
     SELECT TOP(@count)
            Streams.IdOriginal As StreamId,
            Events.StreamVersion,
            Events.Ordinal,
            Events.Id AS EventId,
            Events.Created,
            Events.Type,
            Events.JsonData,
            Events.JsonMetadata
       FROM Events
 INNER JOIN Streams
         ON Events.StreamIdInternal=Streams.IdInternal
      WHERE Events.Ordinal <= @ordinal
   ORDER BY Events.Ordinal DESC;

/* Delete Stream*/
DECLARE @streamIdInternal AS INT;
DECLARE @streamId AS NVARCHAR(40) = 'stream-1'

BEGIN TRANSACTION DeleteStream
         SELECT @streamIdInternal = Streams.IdInternal
           FROM Streams
          WHERE Streams.Id = @streamId;

    DELETE FROM Events
          WHERE Events.StreamIdInternal = @streamIdInternal;
       
         UPDATE Streams
            SET IsDeleted = '1'
          WHERE Streams.Id = @streamId;
COMMIT TRANSACTION DeleteStream

SELECT * FROM dbo.Streams;
SELECT * FROM dbo.Events;

/* ReadStreamForward */
DECLARE @StreamVersion AS INT = 0
DECLARE @isDeleted AS BIT;
SET @streamId = 'stream-2'

     SELECT @streamIdInternal = Streams.IdInternal,
            @isDeleted = Streams.IsDeleted
       FROM Streams
      WHERE Streams.Id = @streamId

     SELECT @isDeleted AS IsDeleted

     SELECT TOP(@count)
            Events.StreamVersion,
            Events.Ordinal,
            Events.Id AS EventId,
            Events.Created,
            Events.Type,
            Events.JsonData,
            Events.JsonMetadata
       FROM Events
      INNER JOIN Streams
         ON Events.StreamIdInternal = Streams.IdInternal
      WHERE Events.StreamIDInternal = @streamIDInternal AND Events.StreamVersion >= @StreamVersion
   ORDER BY Events.Ordinal;

     SELECT TOP(1)
            Events.StreamVersion
       FROM Events
      WHERE Events.StreamIDInternal = @streamIDInternal
   ORDER BY Events.Ordinal DESC;

/* ReadStreamBackward */

SET @StreamVersion = 5;

     SELECT @streamIdInternal = Streams.IdInternal,
            @isDeleted = Streams.IsDeleted
       FROM Streams
      WHERE Streams.Id = @streamId

     SELECT @isDeleted;

     SELECT TOP(@count)
            Streams.IdOriginal As StreamId,
            Streams.IsDeleted as IsDeleted,
            Events.StreamVersion,
            Events.Ordinal,
            Events.Id AS EventId,
            Events.Created,
            Events.Type,
            Events.JsonData,
            Events.JsonMetadata
       FROM Events
 INNER JOIN Streams
         ON Events.StreamIdInternal = Streams.IdInternal
      WHERE Events.StreamIDInternal = @streamIDInternal AND Events.StreamVersion <= @StreamVersion
   ORDER BY Events.Ordinal DESC

     SELECT TOP(1)
            Events.StreamVersion
       FROM Events
      WHERE Events.StreamIDInternal = @streamIDInternal
   ORDER BY Events.Ordinal DESC;

/* Delete Stream with expected version */ 
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION DeleteStream
        DECLARE @streamIdInternal2 AS INT;
        DECLARE @expectedStreamVersion AS INT = 3;
        DECLARE @latestStreamVersion  AS INT;
        SET @streamId = 'stream-1';

         SELECT @streamIdInternal2 = Streams.IdInternal
           FROM Streams
          WHERE Streams.Id = @streamId;

          IF @streamIdInternal2 IS NULL
          BEGIN
             ROLLBACK TRANSACTION DeleteStream;
             RAISERROR('WrongExpectedVersion', 12,1);
          END

          SELECT TOP(1)
                @latestStreamVersion = Events.StreamVersion
           FROM Events
          WHERE Events.StreamIDInternal = @streamIdInternal2
       ORDER BY Events.Ordinal DESC;

         IF @latestStreamVersion != @expectedStreamVersion
         BEGIN
            ROLLBACK TRANSACTION DeleteStream;
            RAISERROR('WrongExpectedVersion', 12,2);
         END

         UPDATE Streams
            SET IsDeleted = '1'
          WHERE Streams.Id = @streamId ;

         DELETE FROM Events
          WHERE Events.StreamIdInternal = @streamIdInternal2;

COMMIT TRANSACTION DeleteStream

SELECT * FROM dbo.Streams;
SELECT * FROM dbo.Events;
