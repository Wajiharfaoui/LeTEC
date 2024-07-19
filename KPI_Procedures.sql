--------------------------------------------------------------------------------------------------------------------------------------------------------------
##### Achat Titre #####
--------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE PROC [CreateAchatTitre]
AS
BEGIN
    IF OBJECT_ID('dbo.pbi_AchatTitre', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.pbi_AchatTitre (
            SalesDate DATE,
            support_type NVARCHAR(100),
            Sales INT,
            unique_cards INT,
            average_sales INT
        )
        WITH (
            DISTRIBUTION = HASH(SalesDate), 
            CLUSTERED COLUMNSTORE INDEX,
            PARTITION (SalesDate RANGE RIGHT FOR VALUES ('2023-01-01', '2024-01-01', '2025-01-01'))
        );

        -- Adding an unenforced unique constraint
        ALTER TABLE dbo.pbi_AchatTitre ADD CONSTRAINT UQ_pbi_AchatTitre UNIQUE (SalesDate, support_type) NOT ENFORCED;
    END

    DECLARE @StartDate DATE = CAST(GETDATE() - 8 AS DATE); -- Start date (7 days ago)
    DECLARE @EndDate DATE = CAST(GETDATE() - 1 AS DATE);   -- End date (yesterday)
    DECLARE @CurrentDate DATE = @StartDate;

    WHILE @CurrentDate <= @EndDate
    BEGIN
        MERGE dbo.pbi_AchatTitre AS target
        USING (
            SELECT 
                sa.bd_transaction_date AS SalesDate, 
                CASE bd_id_support_label
                    WHEN 899240000 THEN 'Billet Papier'
                    WHEN 899240001 THEN 'Carte MOBIB'
                    WHEN 899240002 THEN 'M-Ticketing TEC'
                    WHEN 899240003 THEN 'M-Ticketing SNCB'
                    WHEN 899240004 THEN 'Ticket sans contact'
                END AS support_type, 
                COUNT(sd.bd_sales_transaction_id) AS Sales,
                COUNT(DISTINCT sd.bd_support_serial_number_lookup) AS unique_cards,
                COUNT(sd.bd_sales_transaction_id) / COUNT(DISTINCT sd.bd_support_serial_number_lookup) AS average_sales
            FROM 
                staging.stg_sale_detail sd 
            INNER JOIN 
                staging.stg_support s ON sd.bd_support_serial_number_lookup = s.id 
            INNER JOIN 
                Prod_Sales sa ON sd.bd_sales_transaction_id = sa.bd_sales_transaction_id
            WHERE 
                sd.bd_support_serial_number_lookup IS NOT NULL 
                AND CAST(sa.bd_transaction_date AS DATE) = @CurrentDate
            GROUP BY 
                sa.bd_transaction_date,
                CASE bd_id_support_label
                    WHEN 899240000 THEN 'Billet Papier'
                    WHEN 899240001 THEN 'Carte MOBIB'
                    WHEN 899240002 THEN 'M-Ticketing TEC'
                    WHEN 899240003 THEN 'M-Ticketing SNCB'
                    WHEN 899240004 THEN 'Ticket sans contact'
                END
        ) AS source
        ON (target.SalesDate = source.SalesDate AND target.support_type = source.support_type)
        WHEN MATCHED THEN
            UPDATE SET 
                target.Sales = source.Sales,
                target.unique_cards = source.unique_cards,
                target.average_sales = source.average_sales
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SalesDate, support_type, Sales, unique_cards, average_sales)
            VALUES (source.SalesDate, source.support_type, source.Sales, source.unique_cards, source.average_sales);

        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
    END
END;


--------------------------------------------------------------------------------------------------------------------------------------------------------------
##### Clients Actifs #####
--------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE PROC [CreateClientActifs]
AS
BEGIN
    IF OBJECT_ID('dbo.pbi_ClientActifs', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.pbi_ClientActifs (
            DateOfExecution DATE,
            CurrentMonthActiveClients BIGINT,
            LastMonthActiveClients BIGINT,
            LastYearSameMonthActiveClients BIGINT
        )
        WITH (
            DISTRIBUTION = ROUND_ROBIN,
            CLUSTERED COLUMNSTORE INDEX, 
            PARTITION (DateOfExecution RANGE RIGHT FOR VALUES ('2023-01-01', '2024-01-01', '2025-01-01')) -- Partitioning by execution date
        );
    END

    DECLARE @StartDate DATE = CAST(GETDATE() - 8 AS DATE); -- Start date (7 days ago)
    DECLARE @EndDate DATE = CAST(GETDATE() - 1 AS DATE);   -- End date (yesterday)
    DECLARE @CurrentDate DATE = @StartDate;

    WHILE @CurrentDate <= @EndDate
    BEGIN
        DECLARE @CurrentMonthStart DATE = DATEFROMPARTS(YEAR(@CurrentDate), MONTH(@CurrentDate), 1);
        DECLARE @LastMonthStart DATE = DATEADD(MONTH, -1, @CurrentMonthStart);
        DECLARE @LastYearStart DATE = DATEADD(YEAR, -1, @CurrentMonthStart);
        DECLARE @TwelveMonthsAgo DATE = DATEADD(MONTH, -12, @CurrentDate);

        INSERT INTO dbo.pbi_ClientActifs
        SELECT 
            @CurrentDate AS DateOfExecution,
            COUNT(DISTINCT CASE WHEN CAST(v.DTHR_OPERATION AS DATE) >= @CurrentMonthStart THEN v.bd_linkedcustomer END) AS CurrentMonthActiveClients,
            COUNT(DISTINCT CASE WHEN CAST(v.DTHR_OPERATION AS DATE) >= @LastMonthStart AND CAST(v.DTHR_OPERATION AS DATE) < @CurrentMonthStart THEN v.bd_linkedcustomer END) AS LastMonthActiveClients,
            COUNT(DISTINCT CASE WHEN CAST(v.DTHR_OPERATION AS DATE) >= @LastYearStart AND CAST(v.DTHR_OPERATION AS DATE) < DATEADD(MONTH, 1, @LastYearStart) THEN v.bd_linkedcustomer END) AS LastYearSameMonthActiveClients
        FROM 
            Prod_Validation v
        WHERE 
            CAST(v.DTHR_OPERATION AS DATE) >= @TwelveMonthsAgo
            AND v.bd_mobibnumber IN (SELECT DISTINCT m.bd_mobibnumber FROM Prod_Mobib m);

        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
    END
END;

--------------------------------------------------------------------------------------------------------------------------------------------------------------
##### Clients Actifs Contract #####
--------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE PROC [CreateClientActifsMoMYoYContract]
AS
BEGIN
    IF OBJECT_ID('dbo.pbi_ClientActifsContract', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.pbi_ClientActifsContract (
            DateOfExecution DATE,
            type_abonnement NVARCHAR(250),
            Categorie_age NVARCHAR(10),
            CurrentMonthActiveClients BIGINT,
            LastMonthActiveClients BIGINT,
            LastYearSameMonthActiveClients BIGINT
        )
        WITH (
            DISTRIBUTION = ROUND_ROBIN, 
            CLUSTERED COLUMNSTORE INDEX, 
            PARTITION (DateOfExecution RANGE RIGHT FOR VALUES ('2023-01-01', '2024-01-01', '2025-01-01')) -- Partitioning by execution date
        );
    END

    DECLARE @StartDate DATE = CAST(GETDATE() - 8 AS DATE); -- Start date (7 days ago)
    DECLARE @EndDate DATE = CAST(GETDATE() - 1 AS DATE);   -- End date (yesterday)
    DECLARE @CurrentDate DATE = @StartDate;

    WHILE @CurrentDate <= @EndDate
    BEGIN
        DECLARE @CurrentMonthStart DATE = DATEFROMPARTS(YEAR(@CurrentDate), MONTH(@CurrentDate), 1);
        DECLARE @CurrentMonthEnd DATE = EOMONTH(@CurrentDate);
        DECLARE @LastMonthStart DATE = DATEADD(MONTH, -1, @CurrentMonthStart);
        DECLARE @LastMonthEnd DATE = EOMONTH(@LastMonthStart);
        DECLARE @LastYearStart DATE = DATEADD(YEAR, -1, @CurrentMonthStart);
        DECLARE @LastYearEnd DATE = EOMONTH(@LastYearStart);
        DECLARE @TwelveMonthsAgo DATE = DATEADD(MONTH, -12, @CurrentDate);

        INSERT INTO dbo.pbi_ClientActifsContract
        SELECT 
            @CurrentDate AS DateOfExecution,
            a.bd_productlonglabel AS type_abonnement,
            CASE
                WHEN DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) BETWEEN 0 AND 12 THEN '0-12 ans'
                WHEN DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) BETWEEN 13 AND 18 THEN '12-18 ans'
                WHEN DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) BETWEEN 19 AND 24 THEN '18-24 ans'
                WHEN DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) BETWEEN 25 AND 40 THEN '24-40 ans'
                WHEN DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) BETWEEN 41 AND 65 THEN '40-65 ans'
                ELSE '65+'
            END AS Categorie_age,
            COUNT(DISTINCT CASE WHEN CAST(v.bd_contract_end_date AS DATE) >= @CurrentMonthEnd AND CAST(v.bd_contract_start_date AS DATE) <= @CurrentMonthStart THEN v.bd_contact_linked END) AS CurrentMonthActiveClients,
            COUNT(DISTINCT CASE WHEN CAST(v.bd_contract_end_date AS DATE) >= @LastMonthEnd AND CAST(v.bd_contract_start_date AS DATE) <= @LastMonthStart THEN v.bd_contact_linked END) AS LastMonthActiveClients,
            COUNT(DISTINCT CASE WHEN CAST(v.bd_contract_end_date AS DATE) >= @LastYearEnd AND CAST(v.bd_contract_start_date AS DATE) <= @LastYearStart THEN v.bd_contact_linked END) AS LastYearSameMonthActiveClients
        FROM 
            staging.stg_sale_detail v 
        INNER JOIN 
            staging.stg_contact c ON v.bd_contact_linked = c.Id
        INNER JOIN 
            staging.stg_article a ON v.bd_article_code_lookup = a.bd_articleid
        WHERE 
            CAST(v.bd_contract_start_date AS DATE) >= @TwelveMonthsAgo
        GROUP BY 
            a.bd_productlonglabel,
            CASE
                WHEN DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) BETWEEN 0 AND 12 THEN '0-12 ans'
                WHEN DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) BETWEEN 13 AND 18 THEN '12-18 ans'
                WHEN DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) BETWEEN 19 AND 24 THEN '18-24 ans'
                WHEN DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) BETWEEN 25 AND 40 THEN '24-40 ans'
                WHEN DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) BETWEEN 41 AND 65 THEN '40-65 ans'
                ELSE '65+'
            END;

        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
    END
END;

--------------------------------------------------------------------------------------------------------------------------------------------------------------
##### Clients Age Ligne ##### 
--------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE PROC [ClientsAgeLigne]
AS
BEGIN
    IF OBJECT_ID('dbo.pbi_ClientAgeLigne', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.pbi_ClientAgeLigne (
            DateOfExecution DATE,
            ClientID NVARCHAR(510),
            Age INT,
            LIBEL_LIGNE NVARCHAR(150),
            AgeCategory NVARCHAR(10),
            ClientType NVARCHAR(20)
        )
        WITH (
            DISTRIBUTION = ROUND_ROBIN, 
            CLUSTERED COLUMNSTORE INDEX, 
            PARTITION (DateOfExecution RANGE RIGHT FOR VALUES ('2023-01-01','2024-01-01', '2025-01-01')) 
        );
    END

    DECLARE @StartDate DATE = CAST(GETDATE() - 8 AS DATE); -- Start date (7 days ago)
    DECLARE @EndDate DATE = CAST(GETDATE() - 1 AS DATE);   -- End date (yesterday)
    DECLARE @CurrentDate DATE = @StartDate;

    WHILE @CurrentDate <= @EndDate
    BEGIN
        DECLARE @CurrentMonthStart DATE = DATEFROMPARTS(YEAR(@CurrentDate), MONTH(@CurrentDate), 1);
        DECLARE @LastYearStart DATE = DATEFROMPARTS(YEAR(@CurrentDate) - 1, 1, 1);

        INSERT INTO pbi_ClientAgeLigne
        SELECT DISTINCT
            @CurrentDate AS DateOfExecution,  
            CAST(c.Id AS NVARCHAR(510)) AS ClientID,
            DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) AS Age,
            l.LIBEL_LIGNE,
            CASE
                WHEN DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) <= 12 THEN '0-12 ans'
                WHEN DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) BETWEEN 13 AND 18 THEN '12-18 ans'
                WHEN DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) BETWEEN 19 AND 24 THEN '18-24 ans'
                WHEN DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) BETWEEN 25 AND 40 THEN '24-40 ans'
                WHEN DATEDIFF(YEAR, c.birthdate, @CurrentMonthStart) BETWEEN 41 AND 65 THEN '40-65 ans'
                ELSE '65+'
            END AS AgeCategory,
            'Client Connu' AS ClientType
        FROM Prod_Validation vl
        INNER JOIN staging.stg_contact c ON vl.bd_linkedcustomer = c.Id
        INNER JOIN staging.stg_REF_LIGNE l ON vl.NUM_LIGNE = l.NUM_LIGNE
        WHERE CAST(DTHR_OPERATION AS DATE) = @CurrentDate
        
        UNION ALL 

        SELECT DISTINCT
            @CurrentDate AS DateOfExecution,
            'N/A' AS ClientID,
            NULL AS Age,
            l.LIBEL_LIGNE,
            NULL AS AgeCategory,
            'Client Non Connu' AS ClientType
        FROM Prod_Validation vl
        INNER JOIN staging.stg_REF_LIGNE l ON vl.NUM_LIGNE = l.NUM_LIGNE
        WHERE CAST(DTHR_OPERATION AS DATE) = @CurrentDate
        AND bd_mobibnumber = '0';

        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
    END
END;

--------------------------------------------------------------------------------------------------------------------------------------------------------------
##### Clients Average Validation #####
--------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE PROC [CreateClientAverageValidationsTableBackfill]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartDate DATE = CAST(GETDATE() - 8 AS DATE); -- Start date (7 days ago)
    DECLARE @EndDate DATE = CAST(GETDATE() - 1 AS DATE);   -- End date (yesterday)
    DECLARE @CurrentDate DATE = @StartDate;

    IF OBJECT_ID('dbo.pbi_ClientAverageValidations', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.pbi_ClientAverageValidations (
            ClientID UNIQUEIDENTIFIER,
            AccountCreationDate DATE,
            DateOfExecution DATE,
            ValidationCount FLOAT,
            Weeks INT, 
            AverageValidationsPerWeek FLOAT
        )
        WITH (
            DISTRIBUTION = HASH(ClientID),
            CLUSTERED COLUMNSTORE INDEX,
            PARTITION (DateOfExecution RANGE RIGHT FOR VALUES ('2023-01-01', '2024-01-01', '2025-01-01'))
        );
    END

    TRUNCATE TABLE dbo.pbi_ClientAverageValidations;

    WHILE @CurrentDate <= @EndDate
    BEGIN
        DECLARE @TwelveMonthsAgo DATE = DATEADD(MONTH, -12, @CurrentDate);

        INSERT INTO dbo.pbi_ClientAverageValidations (ClientID, AccountCreationDate, DateOfExecution, ValidationCount, Weeks, AverageValidationsPerWeek)
        SELECT 
            v.bd_linkedcustomer AS ClientID,
            c.createdon AS AccountCreationDate,
            @CurrentDate AS DateOfExecution,
            COUNT(v.validation_id) AS ValidationCount,
            NULLIF(
                CASE 
                    WHEN c.createdon < @TwelveMonthsAgo THEN 52
                    ELSE DATEDIFF(WEEK, c.createdon, @CurrentDate)
                END, 0
            ) AS Weeks,
            CASE 
                WHEN c.createdon < @TwelveMonthsAgo THEN CAST(COUNT(v.validation_id) AS FLOAT) / 52
                ELSE CAST(COUNT(v.validation_id) AS FLOAT) / NULLIF(DATEDIFF(WEEK, c.createdon, @CurrentDate - 1), 0)
            END AS AverageValidationsPerWeek
        FROM 
            Prod_Validation AS v
        INNER JOIN 
            staging.stg_contact AS c ON c.Id = v.bd_linkedcustomer
        WHERE 
            CAST(v.DTHR_OPERATION AS DATE) >= CASE 
                                    WHEN c.createdon < @TwelveMonthsAgo THEN @TwelveMonthsAgo 
                                    ELSE c.createdon 
                                END
        GROUP BY 
            v.bd_linkedcustomer, 
            c.createdon;

        DELETE FROM dbo.pbi_ClientAverageValidations
        WHERE AverageValidationsPerWeek < 4;

        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
    END
END;

--------------------------------------------------------------------------------------------------------------------------------------------------------------
##### Clients Total #####
--------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE PROC [CreateClientTotal]
AS
BEGIN
    IF OBJECT_ID('dbo.pbi_ClientTotal', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.pbi_ClientTotal (
            DateOfExecution DATE,
            type_abonnement NVARCHAR(255),
            total_clients INT
        )
        WITH (
            DISTRIBUTION = ROUND_ROBIN, -- Even distribution of data across all nodes
            CLUSTERED COLUMNSTORE INDEX, -- For better compression and performance
            PARTITION (DateOfExecution RANGE RIGHT FOR VALUES ('2023-01-01', '2024-01-01', '2025-01-01'))-- Partitioning by execution date
        );
    END

    DECLARE @StartDate DATE = CAST(GETDATE() - 8 AS DATE); -- Start date (7 days ago)
    DECLARE @EndDate DATE = CAST(GETDATE() - 1 AS DATE);   -- End date (yesterday)
    DECLARE @CurrentDate DATE = @StartDate;

    WHILE @CurrentDate <= @EndDate
    BEGIN
        INSERT INTO dbo.pbi_ClientTotal
        SELECT @CurrentDate AS DateOfExecution, bd_productlonglabel AS type_abonnement, SUM(clients) AS total_clients
        FROM (
            SELECT COUNT(DISTINCT c.bd_contact_linked) AS clients, bd_productlonglabel
            FROM Prod_Sales AS c
            WHERE CAST(c.bd_transaction_date AS DATE) >= DATEADD(MONTH, -12, @CurrentDate)
                  AND c.bd_contact_linked IS NOT NULL
            GROUP BY bd_productlonglabel
            UNION ALL
            
            SELECT COUNT(DISTINCT sd.bd_sales_transaction_id) AS clients, sd.bd_productlonglabel
            FROM Prod_Sales AS sd
            WHERE CAST(sd.bd_transaction_date AS DATE) >= DATEADD(MONTH, -12, @CurrentDate)
                  AND sd.bd_contact_linked IS NULL
            GROUP BY sd.bd_productlonglabel
        ) AS combined_clients
        GROUP BY bd_productlonglabel;

        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
    END
END;

--------------------------------------------------------------------------------------------------------------------------------------------------------------
##### Delai Achat #####
--------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE PROC [CreateDelaiValidationTable]
AS
BEGIN
    IF OBJECT_ID('dbo.pbi_DelaiAchat', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.pbi_DelaiAchat (
            bd_contact_linked NVARCHAR(1020),
            bd_articlelonglabel NVARCHAR(510),
            bd_support_class NVARCHAR(1020),
            max_transaction_date DATE,
            first_validation_date_after_transaction DATE,
            Delai INT
        )
        WITH (
            DISTRIBUTION = HASH(bd_contact_linked),
            CLUSTERED COLUMNSTORE INDEX,
            PARTITION (max_transaction_date RANGE RIGHT FOR VALUES ('2023-01-01','2024-01-01', '2025-01-01'))
        );
    END

    DECLARE @StartDate DATE = CAST(GETDATE() - 8 AS DATE); -- Start date (7 days ago)
    DECLARE @EndDate DATE = CAST(GETDATE() - 1 AS DATE);   -- End date (yesterday)
    DECLARE @CurrentDate DATE = @StartDate;

    WHILE @CurrentDate <= @EndDate
    BEGIN
        DECLARE @CurrentMonthStart DATE = DATEFROMPARTS(YEAR(@CurrentDate), MONTH(@CurrentDate), 1);

        WITH LastTransactions AS (
            SELECT
                s.bd_contact_linked,
                s.bd_articlelonglabel,
                MAX(s.bd_transaction_date) AS max_transaction_date
            FROM
                prod_sales AS s
            WHERE
                s.bd_contact_linked IS NOT NULL
                AND CAST(s.bd_transaction_date AS DATE) >= @CurrentMonthStart
            GROUP BY
                s.bd_contact_linked, s.bd_articlelonglabel
        ),
        FirstValidations AS (
            SELECT
                v.bd_linkedcustomer,
                v.DTHR_OPERATION AS first_validation_date_after_transaction,
                v.bd_support_class,
                ROW_NUMBER() OVER(PARTITION BY v.bd_linkedcustomer ORDER BY v.DTHR_OPERATION ASC) AS rn
            FROM
                Prod_Validation AS v
            JOIN
                LastTransactions AS lt ON v.bd_linkedcustomer = lt.bd_contact_linked
            WHERE
                CAST(v.DTHR_OPERATION AS DATE) >= @CurrentMonthStart
        )
        INSERT INTO dbo.pbi_DelaiAchat
        SELECT
            lt.bd_contact_linked,
            lt.bd_articlelonglabel,
            fv.bd_support_class,
            lt.max_transaction_date,
            fv.first_validation_date_after_transaction,
            DATEDIFF(day, lt.max_transaction_date, fv.first_validation_date_after_transaction) AS Delai
        FROM
            LastTransactions AS lt
        JOIN
            FirstValidations AS fv ON lt.bd_contact_linked = fv.bd_linkedcustomer AND fv.rn = 1
        WHERE
            fv.first_validation_date_after_transaction >= lt.max_transaction_date;

        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
    END
END;

--------------------------------------------------------------------------------------------------------------------------------------------------------------
##### Clients Validation #####
--------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE PROC [ClientValid] AS 
BEGIN 
	SET NOCOUNT ON; 
	DECLARE @Today DATE = CAST(GETDATE() - 1 AS DATE);
	DECLARE @CurrentMonthStart DATE = DATEFROMPARTS(YEAR(GETDATE()-1), MONTH(GETDATE()-1), 1);
    IF OBJECT_ID('dbo.pbi_ValidationClient', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.pbi_ValidationClient (
            Date_de_validation DATE,
            Type_abonnement NVARCHAR(255),
            Type_support NVARCHAR(255),
            Age INT,
            Categorie_age  NVARCHAR(10) ,
            Nombre_de_validation INT

        )
        WITH (
            DISTRIBUTION = HASH(Type_abonnement), 
            CLUSTERED COLUMNSTORE INDEX, 
            PARTITION (Date_de_validation RANGE RIGHT FOR VALUES ('2023-01-01','2024-01-01','2025-01-01')) 
        );
    END;
   
   with ValidbyAge AS ( 
   SELECT
   	CAST(v.DTHR_OPERATION AS DATE) AS Date_de_validation, 
    v.LIBEL_LONG_PRODUIRE AS Type_abonnement, 
    v.bd_support_class as Type_support,
    DATEDIFF(year, c.birthdate, @CurrentMonthStart) AS Age,
    CASE
	        WHEN DATEDIFF(year, c.birthdate, @CurrentMonthStart) BETWEEN 0 AND 12 THEN '0-12 ans'
	        WHEN DATEDIFF(year, c.birthdate, @CurrentMonthStart) BETWEEN 13 AND 18 THEN '12-18 ans'
	        WHEN DATEDIFF(year, c.birthdate, @CurrentMonthStart) BETWEEN 19 AND 24 THEN '18-24 ans'
	        WHEN DATEDIFF(year, c.birthdate, @CurrentMonthStart) BETWEEN 25 AND 40 THEN '24-40 ans'
	        WHEN DATEDIFF(year, c.birthdate, @CurrentMonthStart) BETWEEN 41 AND 65 THEN '40-65 ans'
	        ELSE '65+'
	    END AS Categorie_age,
    COUNT(distinct validation_id) AS Nombre_de_validation
    FROM Prod_Validation v
    inner join staging.stg_contact c
    ON v.bd_linkedcustomer = c.id
    WHERE 
            CAST(v.DTHR_OPERATION AS DATE) <= @Today 
            AND CAST(v.DTHR_OPERATION AS DATE) >= '2023-01-01' -- L'année courante
        GROUP BY 
    CAST(v.DTHR_OPERATION AS DATE), 
    v.LIBEL_LONG_PRODUIRE, 
    v.bd_support_class,
    DATEDIFF(year, c.birthdate, @CurrentMonthStart),
    CASE
	        WHEN DATEDIFF(year, c.birthdate, @CurrentMonthStart) BETWEEN 0 AND 12 THEN '0-12 ans'
	        WHEN DATEDIFF(year, c.birthdate, @CurrentMonthStart) BETWEEN 13 AND 18 THEN '12-18 ans'
	        WHEN DATEDIFF(year, c.birthdate, @CurrentMonthStart) BETWEEN 19 AND 24 THEN '18-24 ans'
	        WHEN DATEDIFF(year, c.birthdate, @CurrentMonthStart) BETWEEN 25 AND 40 THEN '24-40 ans'
	        WHEN DATEDIFF(year, c.birthdate, @CurrentMonthStart) BETWEEN 41 AND 65 THEN '40-65 ans'
	        ELSE '65+'
	    END)
   INSERT INTO dbo.pbi_ValidationClient
    SELECT Date_de_validation, Type_abonnement,Type_support,Age,Categorie_age, Nombre_de_validation
    FROM ValidbyAge;
END;  

--------------------------------------------------------------------------------------------------------------------------------------------------------------
##### Validation Count #####
--------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE PROC [CreateValidationCountsTable]
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('dbo.pbi_ValidationCounts', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.pbi_ValidationCounts (
            Date_de_validation DATE,
            Type_abonnement NVARCHAR(255),
            Nombre_de_validation INT,
            Nombre_de_validation_annee_precedente INT
        )
        WITH (
            DISTRIBUTION = HASH(Type_abonnement), 
            CLUSTERED COLUMNSTORE INDEX, 
            PARTITION (Date_de_validation RANGE RIGHT FOR VALUES ('2023-01-01', '2024-01-01', '2025-01-01'))
        );
    END;

    DECLARE @StartDate DATE = CAST(GETDATE() - 8 AS DATE); -- Start date (7 days ago)
    DECLARE @EndDate DATE = CAST(GETDATE() - 1 AS DATE);   -- End date (yesterday)
    DECLARE @CurrentDate DATE = @StartDate;

    WHILE @CurrentDate <= @EndDate
    BEGIN
        -- Calculate the counts for the current period
        INSERT INTO dbo.pbi_ValidationCounts (Date_de_validation, Type_abonnement, Nombre_de_validation, Nombre_de_validation_annee_precedente)
        SELECT 
            CAST(DTHR_OPERATION AS DATE) AS Date_de_validation, 
            LIBEL_LONG_PRODUIRE AS Type_abonnement, 
            COUNT(DISTINCT validation_id) AS Nombre_de_validation,
            0 AS Nombre_de_validation_annee_precedente -- Placeholder for now
        FROM 
            Prod_Validation
        WHERE 
            CAST(DTHR_OPERATION AS DATE) = @CurrentDate
        GROUP BY 
            CAST(DTHR_OPERATION AS DATE), 
            LIBEL_LONG_PRODUIRE;

        -- Update counts for the previous year
        UPDATE pbi
        SET pbi.Nombre_de_validation_annee_precedente = pv.PrevYearCount
        FROM dbo.pbi_ValidationCounts pbi
        JOIN (
            SELECT 
                CAST(DATEADD(YEAR, 1, DTHR_OPERATION) AS DATE) AS PrevYearDate,
                LIBEL_LONG_PRODUIRE,
                COUNT(DISTINCT validation_id) AS PrevYearCount
            FROM Prod_Validation
            WHERE 
                CAST(DTHR_OPERATION AS DATE) = DATEADD(YEAR, -1, @CurrentDate)
            GROUP BY 
                CAST(DATEADD(YEAR, 1, DTHR_OPERATION) AS DATE),
                LIBEL_LONG_PRODUIRE
        ) pv ON pbi.Date_de_validation = pv.PrevYearDate AND pbi.Type_abonnement = pv.LIBEL_LONG_PRODUIRE;

        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
    END
END;


--------------------------------------------------------------------------------------------------------------------------------------------------------------
##### Validation Rate per user #####
--------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE PROC [CreateValidationRatePerUserTable]
AS
BEGIN
    -- Optionally, check if the table already exists and drop it
    IF OBJECT_ID('dbo.pbi_ValidationRatePerUser', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.pbi_ValidationRatePerUser (
            ValidationDate DATE,
            AverageValidationsPerUser FLOAT
        )
        WITH (
            CLUSTERED COLUMNSTORE INDEX, -- Best for analytical workloads
            PARTITION (ValidationDate RANGE RIGHT FOR VALUES ('2023-01-01', '2024-01-01','2025-01-01'))
        );
    END;

    DECLARE @StartDate DATE = CAST(GETDATE() - 8 AS DATE); -- Start date (7 days ago)
    DECLARE @EndDate DATE = CAST(GETDATE() - 1 AS DATE);   -- End date (yesterday)
    DECLARE @CurrentDate DATE = @StartDate;

    WHILE @CurrentDate <= @EndDate
    BEGIN
        WITH DailyValidations AS (
            SELECT 
                CAST(DTHR_OPERATION AS DATE) AS ValidationDate,
                bd_linkedcustomer,
                COUNT(*) AS ValidationCount
            FROM 
                Prod_Validation
            WHERE 
                CAST(DTHR_OPERATION AS DATE) = @CurrentDate
            GROUP BY 
                CAST(DTHR_OPERATION AS DATE), bd_linkedcustomer
        ),
        DailyUsers AS (
            SELECT 
                ValidationDate,
                COUNT(DISTINCT bd_linkedcustomer) AS UsersValidated
            FROM 
                DailyValidations
            GROUP BY 
                ValidationDate
        ),
        DailyAverageValidations AS (
            SELECT 
                DV.ValidationDate,
                SUM(DV.ValidationCount) AS TotalValidations,
                DU.UsersValidated,
                CAST(SUM(DV.ValidationCount) AS FLOAT) / DU.UsersValidated AS AverageValidationsPerUser
            FROM 
                DailyValidations DV
            JOIN 
                DailyUsers DU ON DV.ValidationDate = DU.ValidationDate
            GROUP BY 
                DV.ValidationDate, DU.UsersValidated
        )
        INSERT INTO dbo.pbi_ValidationRatePerUser
        SELECT 
            ValidationDate,
            AverageValidationsPerUser
        FROM 
            DailyAverageValidations;

        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
    END
END;

--------------------------------------------------------------------------------------------------------------------------------------------------------------
##### Vente Canal #####   
--------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE PROC [CreateValidationCanal]
AS
BEGIN
    DECLARE @StartDate DATE = CAST(GETDATE() - 8 AS DATE); -- Start date (7 days ago)
    DECLARE @EndDate DATE = CAST(GETDATE() - 1 AS DATE);   -- End date (yesterday)
    DECLARE @CurrentDate DATE = @StartDate;

    IF OBJECT_ID('dbo.pbi_VentesCanal', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.pbi_VentesCanal (
            DateOfExecution DATE,
            EquipementType NVARCHAR(100),
            ProductType NVARCHAR(100),
            SalesQuant INT,
            Sales INT
        )
        WITH (
            DISTRIBUTION = ROUND_ROBIN, 
            CLUSTERED COLUMNSTORE INDEX, 
            PARTITION (DateOfExecution RANGE RIGHT FOR VALUES ('2023-01-01', '2024-01-01', '2025-01-01'))
        );
    END;

    WHILE @CurrentDate <= @EndDate
    BEGIN
        INSERT INTO dbo.pbi_VentesCanal
        SELECT 
            s.bd_transaction_date AS DateOfExecution, 
            CASE 
                WHEN s.bd_equipment_type = 'MG' THEN 'POS (guichet), Application dépositaire'
                WHEN s.bd_equipment_type = 'DI' THEN 'Valideur embarqué'
                WHEN s.bd_equipment_type = 'VM' THEN 'Valideur évènementiel'
                WHEN s.bd_equipment_type = 'TW' THEN 'VAD partenaire de rechargement, site web, Mobile'
                WHEN s.bd_equipment_type = 'PC' THEN 'Portable de contrôle'
                ELSE s.bd_equipment_type 
            END AS EquipmentType, 
            s.bd_productlonglabel AS ProductType,
            COUNT(DISTINCT s.bd_sales_transaction_id) AS SalesQuant,
            SUM(s.bd_global_amount) AS Sales
        FROM 
            Prod_Sales s
        WHERE 
            CAST(s.bd_transaction_date AS DATE) = @CurrentDate
        GROUP BY 
            s.bd_transaction_date, 
            CASE 
                WHEN s.bd_equipment_type = 'MG' THEN 'POS (guichet), Application dépositaire'
                WHEN s.bd_equipment_type = 'DI' THEN 'Valideur embarqué'
                WHEN s.bd_equipment_type = 'VM' THEN 'Valideur évènementiel'
                WHEN s.bd_equipment_type = 'TW' THEN 'VAD partenaire de rechargement, site web, Mobile'
                WHEN s.bd_equipment_type = 'PC' THEN 'Portable de contrôle'
                ELSE s.bd_equipment_type
            END,
            s.bd_productlonglabel;

        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
    END
END;


