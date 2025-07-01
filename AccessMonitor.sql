/*
############################ Unit IT ############################
-- Date             :   2023-02-08
-- Author           :   BHO
-- Tags             :   Unencryptet, AccessMonitor, Migrering
-- Description      :   En ny version uden krypteret SPs, men med Kolonne for om forbindelsen var krypteret
############################ Changelog ##########################
-- Date          Author     Description      
-- 2023-02-08    BHO
############################ Ideas ##############################
-- Date          Initials   Description
*/

USE [master]
GO

-- Create UnitIT Schema
IF NOT EXISTS (SELECT *
FROM sys.schemas
WHERE name = 'UnitIT')
BEGIN
	EXEC ('CREATE SCHEMA UnitIT AUTHORIZATION dbo')
END

DECLARE @ReCreate_AccessMonitorTable NVARCHAR(1)
/* ----------------------------------------- SETTINGS ------------------------------------*/
SET @ReCreate_AccessMonitorTable = 					'Y'
/* ----------------------------------------- END SETTINGS ------------------------------------*/

IF (OBJECT_ID('UnitIT.AccessMonitor', 'U') IS NOT NULL AND @ReCreate_AccessMonitorTable = 'Y') DROP TABLE [UnitIT].[AccessMonitor]
GO
IF (OBJECT_ID('UnitIT.AccessMonitor', 'U') IS NULL)
BEGIN
	CREATE TABLE [UnitIT].[AccessMonitor]
	(
		[Server_Name] VARCHAR(128) NOT NULL,
		[Server_IP] VARCHAR(48) NULL,
		[Database_Id] SMALLINT NOT NULL,
		[Database_Name] VARCHAR(128) NOT NULL,
		[Client_Login_SID] VARBINARY(85) NOT NULL,
		[Client_Login_Name] VARCHAR(128) NOT NULL,
		[Client_Orginal_Login_SID] VARBINARY(85) NULL,
		[Client_Orignal_Login_Name] VARCHAR(128) NULL,
		[Client_WindowsLogin_Domain] VARCHAR(128) NULL,
		[Client_WindowsLogin_UserName] VARCHAR(128) NULL,
		[Client_HostName] VARCHAR(128) NULL,
		[Client_ProgramName] VARCHAR(128) NULL,
		[Client_Driver] VARCHAR(32) NULL, 
		[Client_IP] VARCHAR(48) NULL,
		[Client_AuthenticationProtocol] VARCHAR(40) NULL,
		[Client_TransportProtocol] VARCHAR(40) NULL,
		[Client_TransportEncrypted] VARCHAR(40) NULL,
		[FirstConnection_UTC] DATETIME2 NOT NULL,
		[LastConnection_UTC] DATETIME2 NOT NULL
	)
END
GO
--Drop Index
IF EXISTS(SELECT * FROM sys.indexes WHERE object_id = object_id('UnitIT.AccessMonitor') AND NAME ='IX_LastConnection')
	DROP INDEX [IX_LastConnection] ON [UnitIT].[AccessMonitor];
GO
--Create Index
CREATE NONCLUSTERED INDEX [IX_LastConnection] ON [UnitIT].[AccessMonitor]
(
	[LastConnection_UTC] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)

GO

-- Drop Collector Procedure if it is exists
IF (OBJECT_ID('UnitIT.AccessMonitor_Collector', 'P') IS NOT NULL) DROP PROCEDURE [UnitIT].[AccessMonitor_Collector]
GO
-- Create Collector Procedure
CREATE PROCEDURE [UnitIT].[AccessMonitor_Collector]
AS
BEGIN
	SELECT
		@@SERVERNAME AS [Server_Name]
		,c.[local_net_address] AS [Server_IP]
		,s.[database_id] AS [Database_Id]
		,DB_NAME(s.[database_id]) AS [Database_Name]
		,s.[security_id] AS [Client_Login_SID]
		,s.[login_name] AS [Client_Login_Name]
		,CASE WHEN s.[security_id] != s.[original_security_id] THEN s.[original_security_id] END AS [Client_Orginal_Login_SID]
		,CASE WHEN s.[security_id] != s.[original_security_id] THEN s.[original_login_name] END AS [Client_Orignal_Login_Name]
		,s.[nt_domain] AS [Client_WindowsLogin_Domain]
		,s.[nt_user_name] AS [Client_WindowsLogin_UserName]
		,s.[host_name] AS [Client_HostName]
		,s.[program_name] AS [Client_ProgramName]
		,s.[client_interface_name] AS [Client_Driver]
		,c.[client_net_address] AS [Client_IP]
		,c.[auth_scheme] AS [Client_AuthenticationProtocol]
		,c.[net_transport] AS [Client_TransportProtocol]
		,c.[encrypt_option] AS [Client_TransportEncrypted]
		,SYSUTCDATETIME() AS [TimeStamp]
	INTO	
		#Temp_AccessMonitor_Connections
	FROM
		sys.dm_exec_sessions s 
		LEFT JOIN sys.dm_exec_connections c ON s.session_id = c.session_id
	WHERE 
		s.is_user_process = 1
		AND s.session_id != @@SPID
	GROUP BY
		s.[database_id]
		,s.[security_id]
		,s.[login_name]
		,s.[original_security_id]
		,s.[original_login_name]
		,s.[nt_domain]
		,s.[nt_user_name]
		,s.[host_name]
		,s.[program_name]
		,s.[client_interface_name]
		,c.[client_net_address]
		,c.[local_net_address]
		,c.[auth_scheme]
		,c.[encrypt_option]
		,c.[net_transport]

	-- Removed thoose whoose a field is empty who cannot be (When a connection is made but the other tables are not fully populated yet)
	DELETE FROM #Temp_AccessMonitor_Connections 
	WHERE 
		[Server_Name] IS NULL 
		OR [Database_Id] IS NULL
		OR [Database_Name] IS NULL
		OR [Client_Login_SID] IS NULL
		OR [Client_Login_Name] IS NULL

	-- Update TS on thoose we already know
	UPDATE [UnitIT].[AccessMonitor]
	SET
		[LastConnection_UTC] = [SOURCE].[TimeStamp]
	FROM
		#Temp_AccessMonitor_Connections [SOURCE]
	INNER JOIN [UnitIT].[AccessMonitor] [TARGET]
		ON
			[SOURCE].[Server_Name] = [TARGET].[Server_Name]
			AND ([SOURCE].[Server_IP] = [TARGET].[Server_IP]  OR ([SOURCE].[Server_IP] IS NULL AND [TARGET].[Server_IP] IS NULL))
			AND [SOURCE].[Database_Id] = [TARGET].[Database_Id]
			AND [SOURCE].[Database_Name] = [TARGET].[Database_Name]
			AND [SOURCE].[Client_Login_SID] = [TARGET].[Client_Login_SID]
			AND [SOURCE].[Client_Login_Name] = [TARGET].[Client_Login_Name]
			AND ([SOURCE].[Client_Orginal_Login_SID] = [TARGET].[Client_Orginal_Login_SID] OR ([SOURCE].[Client_Orginal_Login_SID] IS NULL AND [TARGET].[Client_Orginal_Login_SID] IS NULL))
			AND ([SOURCE].[Client_Orignal_Login_Name] = [TARGET].[Client_Orignal_Login_Name] OR ([SOURCE].[Client_Orignal_Login_Name] IS NULL AND [TARGET].[Client_Orignal_Login_Name] IS NULL))
			AND ([SOURCE].[Client_WindowsLogin_Domain] = [TARGET].[Client_WindowsLogin_Domain]  OR ([SOURCE].[Client_WindowsLogin_Domain] IS NULL AND [TARGET].[Client_WindowsLogin_Domain] IS NULL))
			AND ([SOURCE].[Client_WindowsLogin_UserName] = [TARGET].[Client_WindowsLogin_UserName]  OR ([SOURCE].[Client_WindowsLogin_UserName] IS NULL AND [TARGET].[Client_WindowsLogin_UserName] IS NULL))
			AND ([SOURCE].[Client_HostName] = [TARGET].[Client_HostName]  OR ([SOURCE].[Client_HostName] IS NULL AND [TARGET].[Client_HostName] IS NULL))
			AND ([SOURCE].[Client_ProgramName] = [TARGET].[Client_ProgramName]  OR ([SOURCE].[Client_ProgramName] IS NULL AND [TARGET].[Client_ProgramName] IS NULL))
			AND ([SOURCE].[Client_Driver] = [TARGET].[Client_Driver] OR ([SOURCE].[Client_Driver] IS NULL AND [TARGET].[Client_Driver] IS NULL))
			AND ([SOURCE].[Client_IP] = [TARGET].[Client_IP]  OR ([SOURCE].[Client_IP] IS NULL AND [TARGET].[Client_IP] IS NULL))
			AND ([SOURCE].[Client_AuthenticationProtocol] = [TARGET].[Client_AuthenticationProtocol]  OR ([SOURCE].[Client_AuthenticationProtocol] IS NULL AND [TARGET].[Client_AuthenticationProtocol] IS NULL))
			AND ([SOURCE].[Client_TransportProtocol] = [TARGET].[Client_TransportProtocol]  OR ([SOURCE].[Client_TransportProtocol] IS NULL AND [TARGET].[Client_TransportProtocol] IS NULL))
			AND ([SOURCE].[Client_TransportEncrypted] = [TARGET].[Client_TransportEncrypted]  OR ([SOURCE].[Client_TransportEncrypted] IS NULL AND [TARGET].[Client_TransportEncrypted] IS NULL))

	-- Insert the new ones
	INSERT INTO [UnitIT].[AccessMonitor]
		(
			[Server_Name],
			[Server_IP],
			[Database_Id],
			[Database_Name],
			[Client_Login_SID],
			[Client_Login_Name],
			[Client_Orginal_Login_SID],
			[Client_Orignal_Login_Name],
			[Client_WindowsLogin_Domain],
			[Client_WindowsLogin_UserName],
			[Client_HostName],
			[Client_ProgramName],
			[Client_Driver], 
			[Client_IP],
			[Client_AuthenticationProtocol],
			[Client_TransportProtocol],
			[Client_TransportEncrypted],
			[FirstConnection_UTC],
			[LastConnection_UTC]
			)
	SELECT
		[SOURCE].[Server_Name],
		[SOURCE].[Server_IP],
		[SOURCE].[Database_Id],
		[SOURCE].[Database_Name],
		[SOURCE].[Client_Login_SID],
		[SOURCE].[Client_Login_Name],
		[SOURCE].[Client_Orginal_Login_SID],
		[SOURCE].[Client_Orignal_Login_Name],
		[SOURCE].[Client_WindowsLogin_Domain],
		[SOURCE].[Client_WindowsLogin_UserName],
		[SOURCE].[Client_HostName],
		[SOURCE].[Client_ProgramName],
		[SOURCE].[Client_Driver], 
		[SOURCE].[Client_IP],
		[SOURCE].[Client_AuthenticationProtocol],
		[SOURCE].[Client_TransportProtocol],
		[SOURCE].[Client_TransportEncrypted],
		[SOURCE].[TimeStamp] AS [FirstConnection_UTC],
		[SOURCE].[TimeStamp] AS [LastConnection_UTC]
	FROM
		#Temp_AccessMonitor_Connections [SOURCE]
	LEFT JOIN [UnitIT].[AccessMonitor] [TARGET]
		ON
			[SOURCE].[Server_Name] = [TARGET].[Server_Name]
			AND ([SOURCE].[Server_IP] = [TARGET].[Server_IP]  OR ([SOURCE].[Server_IP] IS NULL AND [TARGET].[Server_IP] IS NULL))
			AND [SOURCE].[Database_Id] = [TARGET].[Database_Id]
			AND [SOURCE].[Database_Name] = [TARGET].[Database_Name]
			AND [SOURCE].[Client_Login_SID] = [TARGET].[Client_Login_SID]
			AND [SOURCE].[Client_Login_Name] = [TARGET].[Client_Login_Name]
			AND ([SOURCE].[Client_Orginal_Login_SID] = [TARGET].[Client_Orginal_Login_SID] OR ([SOURCE].[Client_Orginal_Login_SID] IS NULL AND [TARGET].[Client_Orginal_Login_SID] IS NULL))
			AND ([SOURCE].[Client_Orignal_Login_Name] = [TARGET].[Client_Orignal_Login_Name] OR ([SOURCE].[Client_Orignal_Login_Name] IS NULL AND [TARGET].[Client_Orignal_Login_Name] IS NULL))
			AND ([SOURCE].[Client_WindowsLogin_Domain] = [TARGET].[Client_WindowsLogin_Domain]  OR ([SOURCE].[Client_WindowsLogin_Domain] IS NULL AND [TARGET].[Client_WindowsLogin_Domain] IS NULL))
			AND ([SOURCE].[Client_WindowsLogin_UserName] = [TARGET].[Client_WindowsLogin_UserName]  OR ([SOURCE].[Client_WindowsLogin_UserName] IS NULL AND [TARGET].[Client_WindowsLogin_UserName] IS NULL))
			AND ([SOURCE].[Client_HostName] = [TARGET].[Client_HostName]  OR ([SOURCE].[Client_HostName] IS NULL AND [TARGET].[Client_HostName] IS NULL))
			AND ([SOURCE].[Client_ProgramName] = [TARGET].[Client_ProgramName]  OR ([SOURCE].[Client_ProgramName] IS NULL AND [TARGET].[Client_ProgramName] IS NULL))
			AND ([SOURCE].[Client_Driver] = [TARGET].[Client_Driver] OR ([SOURCE].[Client_Driver] IS NULL AND [TARGET].[Client_Driver] IS NULL))
			AND ([SOURCE].[Client_IP] = [TARGET].[Client_IP]  OR ([SOURCE].[Client_IP] IS NULL AND [TARGET].[Client_IP] IS NULL))
			AND ([SOURCE].[Client_AuthenticationProtocol] = [TARGET].[Client_AuthenticationProtocol]  OR ([SOURCE].[Client_AuthenticationProtocol] IS NULL AND [TARGET].[Client_AuthenticationProtocol] IS NULL))
			AND ([SOURCE].[Client_TransportProtocol] = [TARGET].[Client_TransportProtocol]  OR ([SOURCE].[Client_TransportProtocol] IS NULL AND [TARGET].[Client_TransportProtocol] IS NULL))
			AND ([SOURCE].[Client_TransportEncrypted] = [TARGET].[Client_TransportEncrypted]  OR ([SOURCE].[Client_TransportEncrypted] IS NULL AND [TARGET].[Client_TransportEncrypted] IS NULL))
		WHERE [TARGET].[Server_Name] IS NULL

	-- Drop temp table
	DROP TABLE #Temp_AccessMonitor_Connections
END
GO

-- Drop LoginsNotObservedInXDays Procedure if it is exists
IF (OBJECT_ID('UnitIT.AccessMonitor_LoginsNotObservedInXDays', 'P') IS NOT NULL) DROP PROCEDURE [UnitIT].[AccessMonitor_LoginsNotObservedInXDays]
GO
-- Create LoginsNotObservedInXDays Procedure
CREATE PROCEDURE [UnitIT].[AccessMonitor_LoginsNotObservedInXDays]
	@DAYS INT = 30
AS
BEGIN
	DECLARE @OldestDate DATETIME2 = DATEADD(DAY,(0-@DAYS),GETUTCDATE())

	SELECT
		sp.[name]
		,sp.[sid]
		,sp.[type]
		,sp.[type_desc]
		,sp.[create_date]
		,sp.[modify_date]
	FROM 
		master.sys.server_principals sp
	LEFT JOIN 
		(	
		SELECT DISTINCT 
			[Client_Login_SID] [sid] 
		FROM 
			[UnitIT].[AccessMonitor]
		WHERE
			[LastConnection_UTC] > @OldestDate
		UNION
		SELECT DISTINCT 
			[Client_Orginal_Login_SID] [sid] 
		FROM 
			[UnitIT].[AccessMonitor]
		WHERE
			[LastConnection_UTC] > @OldestDate
			AND Client_Orginal_Login_SID IS NOT NULL
		) am 
			ON am.[sid] = sp.[sid]
	WHERE 
		sp.[type] in ('U','S') 
		AND sp.[is_disabled] = 0
		AND am.[sid] IS NULL
		AND sp.[name] NOT LIKE 'NT SERVICE\%'
END
GO

-- Drop RemoteLoginsInTheLastXDays Procedure if it is exists
IF (OBJECT_ID('UnitIT.AccessMonitor_LoginsInTheLastXDays', 'P') IS NOT NULL) DROP PROCEDURE [UnitIT].[AccessMonitor_LoginsInTheLastXDays]
GO
-- Create LoginsInTheLastXDays Procedure
CREATE PROCEDURE [UnitIT].[AccessMonitor_LoginsInTheLastXDays]
	@DAYS INT = 30
	,@ShowOnlyUnsecure BIT = 0
AS
BEGIN
	DECLARE @OldestDate DATETIME2 = DATEADD(DAY,(0-@DAYS),GETUTCDATE())

	IF(@ShowOnlyUnsecure = 1)
	BEGIN
		SELECT DISTINCT [Database_Name]
		  ,[Client_Login_Name]
		  ,[Client_HostName]
		  ,[Client_ProgramName]
		  ,[Client_Driver]
		  ,[Client_IP]
		  ,[Client_AuthenticationProtocol]
		  ,[Client_TransportProtocol]
		  ,[Client_TransportEncrypted]
		  ,[LastConnection_UTC]
	  FROM [UnitIT].[AccessMonitor]
	  WHERE 
		[LastConnection_UTC] > @OldestDate
		AND [Client_TransportEncrypted] = 'FALSE'
		AND [Client_TransportProtocol] = 'TCP'
	END
	ELSE
	BEGIN 
		SELECT DISTINCT [Database_Name]
		  ,[Client_Login_Name]
		  ,[Client_HostName]
		  ,[Client_ProgramName]
		  ,[Client_Driver]
		  ,[Client_IP]
		  ,[Client_AuthenticationProtocol]
		  ,[Client_TransportProtocol]
		  ,[Client_TransportEncrypted]
		  ,[LastConnection_UTC]
	  FROM [UnitIT].[AccessMonitor]
	  WHERE 
		[LastConnection_UTC] > @OldestDate
	END
END
GO

-- Drop DatabasesNotConnectedInTheLastXDays Procedure if it is exists
IF (OBJECT_ID('UnitIT.AccessMonitor_DatabasesNotConnectedInTheLastXDays', 'P') IS NOT NULL) DROP PROCEDURE [UnitIT].[AccessMonitor_DatabasesNotConnectedInTheLastXDays]
GO
-- Create DatabasesNotConnectedInTheLastXDays Procedure
CREATE PROCEDURE [UnitIT].[AccessMonitor_DatabasesNotConnectedInTheLastXDays]
	@DAYS INT = 30
AS
BEGIN
	DECLARE @OldestDate DATETIME2 = DATEADD(DAY,(0-@DAYS),GETUTCDATE())

	SELECT 
		db.[name] [Name]
	FROM 
		master.sys.databases db
	LEFT JOIN (
		SELECT DISTINCT 
			[Database_Name] [name]
		FROM 
			[UnitIT].[AccessMonitor]
		WHERE
			[LastConnection_UTC] > @OldestDate
		) am ON am.[name] = db.[name]
	WHERE
		am.[name] IS NULL
		AND db.[name] NOT IN ('master','tempdb','model','msdb')
END
GO

-- Remove Job if it exists
IF EXISTS (SELECT job_id
FROM msdb.dbo.sysjobs_view
WHERE name = N'Unit IT - Access Monitor')
EXEC msdb.dbo.sp_delete_job @job_name=N'Unit IT - Access Monitor', @delete_unused_schedule=1
GO

-- Add Job
EXEC  msdb.dbo.sp_add_job
		@job_name = N'Unit IT - Access Monitor'
		, @enabled = 1
		, @owner_login_name = N'sa'
GO

-- Add Job To Server
EXEC msdb.dbo.sp_add_jobserver
	@job_name = N'Unit IT - Access Monitor'
GO

DECLARE @DBNAME NVARCHAR(128)
SELECT @DBNAME = DB_NAME()
-- Add Job Step
EXEC msdb.dbo.sp_add_jobstep
	@job_name = N'Unit IT - Access Monitor'
	, @step_name = N'Start AccessMonitor'
	, @step_id = 1
	, @os_run_priority = 0
	, @subsystem = N'TSQL'
	, @command = N'EXEC [UnitIT].[AccessMonitor_Collector]'
	, @database_name = @DBNAME 
GO

EXEC msdb.dbo.sp_add_jobschedule
		@job_name = N'Unit IT - Access Monitor'
		, @name = N'Unit IT - Access Monitor'
		, @enabled = 1
		, @freq_type = 4
		, @freq_interval = 1
		, @freq_subday_type = 2
		, @freq_subday_interval = 10
		, @freq_relative_interval = 0
		, @freq_recurrence_factor = 0
GO