                                                      /*		 SQL QUERY FOR PAYMENT ANALYSIS        */
													  /*         CURRENT EXECUTION TIME - 00:17:09     */ 
/*********** COLLECTS REQUIRED DATA FROM PAYMENT TABLE *************/
SELECT PT.[PaymentID]
      --,PT.[LoanID]
      ,PT.[OriginalLoanID]
      --,PT.[MerchantId]
      --,PT.[Merchant]
      ,PT.[customerId]
      ,PT.[SocialSecurityNumber]
      --,PT.[PaidPrincipal]
     --,PT.[PaidFinanceFee]
      --,PT.[PaidFeeCharges]
      --,PT.[PaymentAmount]
      --,PT.[PaymentModeID]
      --,PT.[PaymentMode]
      --,PT.[PaymentTypeID]
      --,PT.[PaymentType]
      --,PT.[IsCredit]
      --,PT.[IsDebit]
      --,PT.[IsAdjustment]
      ,PT.[PaymentStatusId]
      --,PT.[PaymentStatus]
      ,PT.[ReturnCode]
      --,PT.[IsManual]
      --,PT.[EffectiveDate]
      ,PT.[PaymentDate]
      --,PT.[ReturnDate]
      --,PT.[SentStatus]
      --,PT.[OriginalPaymentId]
      --,PT.[ReturnedPaymentId]
      --,PT.[CreatedDate]
      --,PT.[CreatedBy]
      --,PT.[ProviderId]
      ,PT.[ProviderName]
      --,PT.[ACHNumber]
 INTO #tmpPaymentTable 
 FROM 
(SELECT 
ROW_NUMBER() OVER(PARTITION BY [SocialSecurityNumber] ORDER BY [OriginationDate] DESC) AS 'RowNumber',
LoanId,
LoanStatusId
FROM 
[dbo].[view_FCL_Loan]
WHERE MerchantId IN (15,18)
) AS Loan 
 LEFT JOIN [dbo].[view_FCL_LoanStatus] LS ON Loan.LoanStatusID = LS.LoanStatusID
 LEFT JOIN [dbo].[view_FCL_Payment] PT ON Loan.LoanId = PT.OriginalLoanID
 --WHERE L.OriginationDate >= '01/01/18'
 WHERE Loan.RowNumber = 1 --PT.MerchantId IN (15,18)
 AND PT.PaymentStatus <> 'Canceled'
 AND PT.IsDebit = 1
 AND LS.IsOpen = 1
 ORDER BY PT.SocialSecurityNumber , PT.PaymentDate DESC

/********* ADD REQUIRED FIELDS TO THE TABLE **********/
ALTER TABLE #tmpPaymentTable 
ADD 
LastAttemptReturnsCnt TINYINT,
LastAttemptSettledCnt TINYINT,
FuturePaymentScheduled TINYINT,
PendingSentStatus TINYINT,
TotalPaymentAttemptCnt TINYINT,
LastPaymentAttemptDate DATE,
LastAttemptReturnCode VARCHAR(50),
LastPaymentAttemptVendorName NCHAR(50)

SET NOCOUNT ON
SET ANSI_WARNINGS OFF


/****************** CALCULATE FIELDS *******************/
DECLARE @SSNCursor CURSOR 
DECLARE @SSN NCHAR(9)
DECLARE @PaymentStatusId SMALLINT
DECLARE @PaymentId NUMERIC(18,0)
DECLARE @PaymentStatusCursor CURSOR
DECLARE @Count SMALLINT
DECLARE @DefaultValue TINYINT
DECLARE @Prior TINYINT
DECLARE @Quit TINYINT
DECLARE @LastPaymentAttemptDate DATE
DECLARE @LastAttemptReturnCode VARCHAR(50)
DECLARE @LastPaymentAttemptVendorName NCHAR(50)
SET @DefaultValue = 1
BEGIN
	SET @SSNCursor = CURSOR FOR 
		SELECT DISTINCT SocialSecurityNumber 
		FROM #tmpPaymentTable

	OPEN @SSNCursor
	FETCH NEXT FROM @SSNCursor
	INTO @SSN

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @PaymentStatusId = (SELECT TOP 1 PaymentStatusId	
							   FROM #tmpPaymentTable 
							   WHERE SocialSecurityNumber = @SSN
							   ORDER BY PaymentDate DESC)

/**************** CALCULATE LAST ATTEMPT RETURNED FIELD ***********************/
		IF @PaymentStatusId = 4 OR @PaymentStatusId = 5
			BEGIN
			SET @LastPaymentAttemptDate = (SELECT TOP 1 PaymentDate
										   FROM #tmpPaymentTable 
									       WHERE SocialSecurityNumber = @SSN
									       ORDER BY PaymentDate DESC)
			SET @LastAttemptReturnCode =  (SELECT TOP 1 ReturnCode
										   FROM #tmpPaymentTable 
									       WHERE SocialSecurityNumber = @SSN
									       ORDER BY PaymentDate DESC)
            SET @LastPaymentAttemptVendorName = (SELECT TOP 1 ProviderName
												 FROM #tmpPaymentTable 
												 WHERE SocialSecurityNumber = @SSN
												 ORDER BY PaymentDate DESC)


			SET @PaymentStatusCursor = CURSOR FOR 
						SELECT PaymentStatusId , PaymentId
						FROM #tmpPaymentTable
						WHERE SocialSecurityNumber = @SSN
						ORDER BY PaymentDate DESC 

				OPEN @PaymentStatusCursor
				FETCH NEXT FROM @PaymentStatusCursor
				INTO @PaymentStatusId ,@PaymentId
		 
				WHILE @@FETCH_STATUS = 0
				BEGIN
					IF @PaymentStatusId = 4 OR @PaymentStatusId = 5
						UPDATE #tmpPaymentTable 
						SET LastAttemptReturnsCnt = @DefaultValue,
							LastPaymentAttemptDate = @LastPaymentAttemptDate,
							LastAttemptReturnCode = @LastAttemptReturnCode,
							LastPaymentAttemptVendorName = @LastPaymentAttemptVendorName 
						WHERE PaymentID = @PaymentId
					ELSE
						BREAK;
				 
					FETCH NEXT FROM @PaymentStatusCursor 
					INTO @PaymentStatusId , @PaymentId
				END
				CLOSE @PaymentStatusCursor 
				DEALLOCATE @PaymentStatusCursor
			END
			
/**************** CALCULATE LAST ATTEMPT SETTLED FIELD ***********************/
		IF @PaymentStatusId = 3
			BEGIN
			SET @DefaultValue = 1
			SET @LastPaymentAttemptDate = (SELECT TOP 1 PaymentDate
										   FROM #tmpPaymentTable 
									       WHERE SocialSecurityNumber = @SSN
									       ORDER BY PaymentDate DESC)
			/*SET @LastAttemptReturnCode =  (SELECT TOP 1 ReturnCode
										   FROM #tmpPaymentTable 
									       WHERE SocialSecurityNumber = @SSN
									       ORDER BY PaymentDate DESC) */
            SET @LastPaymentAttemptVendorName = (SELECT TOP 1 ProviderName
												 FROM #tmpPaymentTable 
												 WHERE SocialSecurityNumber = @SSN
												 ORDER BY PaymentDate DESC)

			SET @PaymentStatusCursor = CURSOR FOR 
						SELECT PaymentStatusId , PaymentId
						FROM #tmpPaymentTable
						WHERE SocialSecurityNumber = @SSN
						ORDER BY PaymentDate DESC 

				OPEN @PaymentStatusCursor
				FETCH NEXT FROM @PaymentStatusCursor
				INTO @PaymentStatusId ,@PaymentId
		 
				WHILE @@FETCH_STATUS = 0
				BEGIN
					IF @PaymentStatusId = 3
						UPDATE #tmpPaymentTable 
						SET LastAttemptSettledCnt = @DefaultValue, 
							LastPaymentAttemptDate = @LastPaymentAttemptDate,
							--LastAttemptReturnCode = @LastAttemptReturnCode,
							LastPaymentAttemptVendorName = @LastPaymentAttemptVendorName 
						WHERE PaymentID = @PaymentId
					ELSE
						BREAK;
				 
					FETCH NEXT FROM @PaymentStatusCursor 
					INTO @PaymentStatusId , @PaymentId
				END
				CLOSE @PaymentStatusCursor 
				DEALLOCATE @PaymentStatusCursor
			END

/**************** CALCULATE FUTURE PAYMENT SCHEDULED FIELD ******************/ 
		IF @PaymentStatusId = 1
			BEGIN
			SET @Quit = 0
			SET @DefaultValue = 1
			SET @PaymentStatusCursor = CURSOR FOR 
						SELECT PaymentStatusId , PaymentId
						FROM #tmpPaymentTable
						WHERE SocialSecurityNumber = @SSN
						ORDER BY PaymentDate DESC 

				OPEN @PaymentStatusCursor
				FETCH NEXT FROM @PaymentStatusCursor
				INTO @PaymentStatusId ,@PaymentId
		 
				WHILE @@FETCH_STATUS = 0
				BEGIN
					IF @Quit = 1
						BREAK;

					IF @PaymentStatusId = 1
						UPDATE #tmpPaymentTable SET FuturePaymentScheduled = @DefaultValue 
						WHERE PaymentID = @PaymentId

					IF @PaymentStatusId = 4 OR @PaymentStatusId = 5
								BEGIN
									SET @LastPaymentAttemptDate = (SELECT TOP 1 PaymentDate
																   FROM #tmpPaymentTable 
																   WHERE PaymentID = @PaymentId)
									SET @LastAttemptReturnCode =  (SELECT TOP 1 ReturnCode
																   FROM #tmpPaymentTable 
									                               WHERE PaymentID = @PaymentId)
									SET @LastPaymentAttemptVendorName = (SELECT TOP 1 ProviderName
																	     FROM #tmpPaymentTable 
																		 WHERE PaymentID = @PaymentId)
									WHILE @@FETCH_STATUS = 0
										BEGIN
											IF @PaymentStatusId = 4 or @PaymentStatusId = 5
												UPDATE #tmpPaymentTable 
												SET LastAttemptReturnsCnt = @DefaultValue,
													LastPaymentAttemptDate = @LastPaymentAttemptDate,
													LastAttemptReturnCode = @LastAttemptReturnCode,
													LastPaymentAttemptVendorName = @LastPaymentAttemptVendorName  
												WHERE PaymentID = @PaymentId
											ELSE
												BEGIN
													SET @Quit = 1
													BREAK;
												END

											FETCH NEXT FROM @PaymentStatusCursor 
											INTO @PaymentStatusId , @PaymentId

										END
								END
					ELSE
						BEGIN
							SET @LastPaymentAttemptDate = (SELECT TOP 1 PaymentDate
															FROM #tmpPaymentTable 
															WHERE PaymentID = @PaymentId)
							/*SET @LastAttemptReturnCode =  (SELECT TOP 1 ReturnCode
															FROM #tmpPaymentTable 
									                         WHERE PaymentID = @PaymentId)*/
							SET @LastPaymentAttemptVendorName = (SELECT TOP 1 ProviderName
																 FROM #tmpPaymentTable 
																 WHERE PaymentID = @PaymentId)						
							IF @PaymentStatusId = 3
								BEGIN 
									WHILE @@FETCH_STATUS = 0
										BEGIN
											IF @PaymentStatusId = 3
												UPDATE #tmpPaymentTable 
												SET LastAttemptSettledCnt = @DefaultValue,
													LastPaymentAttemptDate = @LastPaymentAttemptDate,
													--LastAttemptReturnCode = @LastAttemptReturnCode,
													LastPaymentAttemptVendorName = @LastPaymentAttemptVendorName  
												WHERE PaymentID = @PaymentId
											ELSE
												BEGIN
													SET @Quit = 1 
													BREAK;
												END
												
											FETCH NEXT FROM @PaymentStatusCursor 
											INTO @PaymentStatusId , @PaymentId
											
										END
								END	
							END			

					FETCH NEXT FROM @PaymentStatusCursor 
					INTO @PaymentStatusId , @PaymentId

				END
			CLOSE @PaymentStatusCursor 
			DEALLOCATE @PaymentStatusCursor
		END


/**************** CALCULATE PENDING SENT STATUS FIELD **********************/ 
		IF @PaymentStatusId = 2
		BEGIN
			SET @Quit = 0
			SET @DefaultValue = 1
			SET @PaymentStatusCursor = CURSOR FOR 
						SELECT PaymentStatusId , PaymentId
						FROM #tmpPaymentTable
						WHERE SocialSecurityNumber = @SSN
						ORDER BY PaymentDate DESC 

				OPEN @PaymentStatusCursor
				FETCH NEXT FROM @PaymentStatusCursor
				INTO @PaymentStatusId ,@PaymentId
		 
				WHILE @@FETCH_STATUS = 0
				BEGIN
					IF @Quit = 1
						BREAK;

					IF @PaymentStatusId = 2
						UPDATE #tmpPaymentTable SET PendingSentStatus = @DefaultValue 
						WHERE PaymentID = @PaymentId

					IF @PaymentStatusId = 4 OR @PaymentStatusId = 5
								BEGIN
									SET @LastPaymentAttemptDate = (SELECT TOP 1 PaymentDate
																   FROM #tmpPaymentTable 
																   WHERE PaymentID = @PaymentId)
									SET @LastAttemptReturnCode =  (SELECT TOP 1 ReturnCode
																   FROM #tmpPaymentTable 
									                               WHERE PaymentID = @PaymentId)
									SET @LastPaymentAttemptVendorName = (SELECT TOP 1 ProviderName
																	     FROM #tmpPaymentTable 
																		 WHERE PaymentID = @PaymentId)
									WHILE @@FETCH_STATUS = 0
										BEGIN
											IF @PaymentStatusId = 4 or @PaymentStatusId = 5
												UPDATE #tmpPaymentTable 
												SET LastAttemptReturnsCnt = @DefaultValue,
													LastPaymentAttemptDate = @LastPaymentAttemptDate,
													LastAttemptReturnCode = @LastAttemptReturnCode,
													LastPaymentAttemptVendorName = @LastPaymentAttemptVendorName  
												WHERE PaymentID = @PaymentId
											ELSE
												BEGIN
													SET @Quit = 1
													BREAK;
												END

											FETCH NEXT FROM @PaymentStatusCursor 
											INTO @PaymentStatusId , @PaymentId

										END
								END
					ELSE
						BEGIN						
							IF @PaymentStatusId = 3
								BEGIN
									SET @LastPaymentAttemptDate = (SELECT TOP 1 PaymentDate
																   FROM #tmpPaymentTable 
															       WHERE PaymentID = @PaymentId)
									/*SET @LastAttemptReturnCode =  (SELECT TOP 1 ReturnCode
																   FROM #tmpPaymentTable 
									                               WHERE PaymentID = @PaymentId)*/
									SET @LastPaymentAttemptVendorName = (SELECT TOP 1 ProviderName
																	     FROM #tmpPaymentTable 
																		 WHERE PaymentID = @PaymentId)
										    
									WHILE @@FETCH_STATUS = 0
										BEGIN
											IF @PaymentStatusId = 3
												UPDATE #tmpPaymentTable 
												SET LastAttemptSettledCnt = @DefaultValue,
													LastPaymentAttemptDate = @LastPaymentAttemptDate,
													--LastAttemptReturnCode = @LastAttemptReturnCode,
													LastPaymentAttemptVendorName = @LastPaymentAttemptVendorName  
												WHERE PaymentID = @PaymentId
											ELSE
												BEGIN
													SET @Quit = 1 
													BREAK;
												END
												
											FETCH NEXT FROM @PaymentStatusCursor 
											INTO @PaymentStatusId , @PaymentId
											
										END
								END	
							END			

					FETCH NEXT FROM @PaymentStatusCursor 
					INTO @PaymentStatusId , @PaymentId

				END
			CLOSE @PaymentStatusCursor 
			DEALLOCATE @PaymentStatusCursor
		END
		
		FETCH NEXT FROM @SSNCursor 
		INTO @SSN
		
	END
	CLOSE @SSNCursor 
	DEALLOCATE @SSNCursor
END			
			
	
/*************** GET COUNTS **************/
SELECT
PT.OriginalLoanID, 
--PT.SocialSecurityNumber,
ISNULL(SUM(PT.LastAttemptReturnsCnt),0) AS LastAttemptReturnsCnt ,
ISNULL(SUM(PT.LastAttemptSettledCnt),0) AS LastAttemptSettledCnt, 
ISNULL(SUM(PT.FuturePaymentScheduled),0) AS FuturePaymentScheduled  , 
ISNULL(SUM(PT.PendingSentStatus),0) AS PendingSentStatus,
SUM(CASE WHEN PT.PaymentStatusID = 4 THEN 1 WHEN PT.PaymentStatusID = 5 THEN 1 ELSE 0 END) AS TotalReturnsCnt,
SUM(CASE WHEN PT.PaymentStatusID = 3 THEN 1 ELSE 0 END) AS TotalSettledCnt,
COUNT(PaymentStatusID) AS TotalPaymentAttemptsCnt,
MAX(PT.LastPaymentAttemptDate) AS LastPaymentAttemptDate,
MAX(PT.LastAttemptReturnCode) AS LastAttemptReturnCode,
MAX(PT.LastPaymentAttemptVendorName) AS LastPaymentAttemptVendorName
INTO #tmpTable
FROM #tmpPaymentTable PT 
--GROUP BY PT.SocialSecurityNumber
GROUP BY PT.OriginalLoanID
--ORDER BY PT.SocialSecurityNumber

SET NOCOUNT ON


/*********************** ANY CHANGES SUCH AS ADDING FIELDS FROM DIFFERENT TABLES CAN BE DONE IN THE BELOW SECTION **************************/
SELECT 
O.SocialSecurityNumber,
O.FirstName,
O.LastName,
O.CustomerId,
T.OriginalLoanID,
--CONVERT(DATE,O.[NextPayDate]) AS NextPayDate,
CONVERT(DATE,P.[CalculatedNextPayDate]) AS NextDueDate,
REPLACE (O.LoanStatus, ',', ' ') AS LoanStatus,
O.Merchant,
CONVERT(DATE,O.OriginationDate) AS OriginationDate,
ISNULL (O.IsFirstDefault, '0') as IsFirstDefault,
O.PreferredPaymentTypeId,
O.PreferredPaymentTypeDesc,
T.LastAttemptReturnsCnt ,
T.LastAttemptSettledCnt, 
T.FuturePaymentScheduled  , 
T.PendingSentStatus,
T.TotalReturnsCnt,
T.TotalSettledCnt,
T.TotalPaymentAttemptsCnt,
T.LastPaymentAttemptDate,
T.LastAttemptReturnCode,
T.LastPaymentAttemptVendorName,
Category.SubCategory
FROM
#tmpTable T
LEFT JOIN (
SELECT 
ROW_NUMBER() OVER(PARTITION BY LoanId ORDER BY TimeAdded DESC) AS 'RowNumber',
LoanId,
TimeAdded,
SubCategory 
FROM 
[FreedomCashLenders].[dbo].[view_FCL_Notes]
WHERE MerchantId IN (15,18)
) AS Category ON T.OriginalLoanID = Category.LoanId
LEFT JOIN  [dbo].[view_FCL_Loan] O ON T.OriginalLoanID = O.LoanId
JOIN [dbo].[view_FCL_CustomerPayDate] P ON O.CustomerId  = P.CustomerId
WHERE Category.RowNumber = 1
ORDER BY O.SocialSecurityNumber
/**************************************************************************************************************************************************/

DROP TABLE #tmpTable
DROP TABLE #tmpPaymentTable