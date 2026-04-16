
/*
===============================================================
Create Database and Schemas
===============================================================
Script Purpose:
      This script creates a new database named 'DataWarehouse' after checking if it already exists.
      If the database exists, it is dropped and recreated. Additionally,the scripts sets up three schemas
      within the database: 'bronze', 'silver', 'gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted. People with caution and ensure you have proper backups before running this script.
*/

--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.database WHERE name = 'DataWare')
  BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

--Create the 'DataWarehouse' database
Create Database DataWarehouse;

use DataWarehouse;
GO

  --Create Schemas
Create SCHEMA bronze;
GO

Create SCHEMA silver;
GO

Create SCHEMA gold;
GO
