import ballerina/http;
import ballerina/sql;
import ballerina/time;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

// ── DB config (read from Config.toml) ──
type DbConfig record {|
    string host;
    int port;
    string user;
    string password;
    string database;
|};

configurable DbConfig dbConfig = ?;

// ── DB client (singleton) ──
final mysql:Client dbClient = check new (
    host     = dbConfig.host,
    port     = dbConfig.port,
    user     = dbConfig.user,
    password = dbConfig.password,
    database = dbConfig.database
);

// ── Response types ──

type AccountStatus "ACTIVE"|"INACTIVE"|"FROZEN";
type AccountType   "SAVINGS"|"CURRENT"|"FIXED";
type TxnType       "CREDIT"|"DEBIT";

type BalanceResponse record {|
    string  accountNumber;
    string  accountName;
    AccountType accountType;
    decimal balance;
    AccountStatus status;
    string  currency;
    string  timestamp;
|};

type NameEnquiryResponse record {|
    string accountNumber;
    string accountName;
    AccountType accountType;
    AccountStatus status;
|};

type Transaction record {|
    string  txnRef;
    TxnType txnType;
    decimal amount;
    string  description;
    decimal balanceAfter;
    string  txnDate;
|};

type StatementResponse record {|
    string        accountNumber;
    string        accountName;
    decimal       currentBalance;
    string        currency;
    Transaction[] transactions;
    int           totalRecords;
|};

type ErrorResponse record {|
    string code;
    string message;
|};

// ── Request types ──

type BalanceRequest record {|
    string accountNumber;
|};

type NameEnquiryRequest record {|
    string accountNumber;
|};

type StatementRequest record {|
    string  accountNumber;
    int     maxRecords = 10;
    string? fromDate   = ();
    string? toDate     = ();
|};

// ── Helper: fetch account row ──
function getAccount(string accountNumber) returns record {|
    string account_number;
    string account_name;
    string account_type;
    decimal balance;
    string status;
|}|error {
    return check dbClient->queryRow(
        `SELECT account_number, account_name, account_type, balance, status
           FROM accounts
          WHERE account_number = ${accountNumber}`
    );
}

// ── Service ───
service /api on new http:Listener(8080) {

    // 1. POST /api/accounts/balance
    resource function post accounts/balance(@http:Payload BalanceRequest req)
            returns BalanceResponse|http:NotFound|http:Forbidden|http:InternalServerError {

        var row = getAccount(req.accountNumber);
        if row is sql:NoRowsError {
            return <http:NotFound>{
                body: <ErrorResponse>{code: "ACCOUNT_NOT_FOUND", message: "Account not found."}
            };
        }
        if row is error {
            return <http:InternalServerError>{
                body: <ErrorResponse>{code: "DB_ERROR", message: row.message()}
            };
        }

        if row.status == "FROZEN" {
            return <http:Forbidden>{
                body: <ErrorResponse>{code: "ACCOUNT_FROZEN", message: "This account is currently frozen."}
            };
        }

        return {
            accountNumber: row.account_number,
            accountName:   row.account_name,
            accountType:   <AccountType>row.account_type,
            balance:       row.balance,
            status:        <AccountStatus>row.status,
            currency:      "NGN",
            timestamp:     time:utcToString(time:utcNow())
        };
    }

    // 2. POST /api/accounts/name
    resource function post accounts/name(@http:Payload NameEnquiryRequest req)
            returns NameEnquiryResponse|http:NotFound|http:InternalServerError {

        var row = getAccount(req.accountNumber);
        if row is sql:NoRowsError {
            return <http:NotFound>{
                body: <ErrorResponse>{code: "ACCOUNT_NOT_FOUND", message: "Account not found."}
            };
        }
        if row is error {
            return <http:InternalServerError>{
                body: <ErrorResponse>{code: "DB_ERROR", message: row.message()}
            };
        }

        return {
            accountNumber: row.account_number,
            accountName:   row.account_name,
            accountType:   <AccountType>row.account_type,
            status:        <AccountStatus>row.status
        };
    }

    // 3. POST /api/accounts/statement
    resource function post accounts/statement(@http:Payload StatementRequest req)
            returns StatementResponse|http:NotFound|http:BadRequest|http:InternalServerError {

        // Validate account exists
        var accountRow = getAccount(req.accountNumber);
        if accountRow is sql:NoRowsError {
            return <http:NotFound>{
                body: <ErrorResponse>{code: "ACCOUNT_NOT_FOUND", message: "Account not found."}
            };
        }
        if accountRow is error {
            return <http:InternalServerError>{
                body: <ErrorResponse>{code: "DB_ERROR", message: accountRow.message()}
            };
        }

        // Validate maxRecords
        if req.maxRecords < 1 || req.maxRecords > 100 {
            return <http:BadRequest>{
                body: <ErrorResponse>{code: "INVALID_LIMIT", message: "Limit must be between 1 and 100."}
            };
        }

        // Build query with optional date filter
        sql:ParameterizedQuery sqlQuery = `SELECT txn_ref, txn_type, amount, description, balance_after,
                        DATE_FORMAT(txn_date, '%Y-%m-%dT%H:%i:%s') AS txn_date
                   FROM transactions
                  WHERE account_number = ${req.accountNumber}
                  ORDER BY txn_date DESC
                  LIMIT ${req.maxRecords}`;

        if req.fromDate is string && req.toDate is string {
            string fromDate = <string>req.fromDate;
            string toDate   = <string>req.toDate;
            sqlQuery = `SELECT txn_ref, txn_type, amount, description, balance_after,
                        DATE_FORMAT(txn_date, '%Y-%m-%dT%H:%i:%s') AS txn_date
                   FROM transactions
                  WHERE account_number = ${req.accountNumber}
                    AND txn_date BETWEEN ${fromDate} AND ${toDate}
                  ORDER BY txn_date DESC
                  LIMIT ${req.maxRecords}`;
        }

        stream<record {|
            string  txn_ref;
            string  txn_type;
            decimal amount;
            string  description;
            decimal balance_after;
            string  txn_date;
        |}, error?> txnStream = dbClient->query(sqlQuery);

        Transaction[]|error txnsResult = from var row in txnStream
            select {
                txnRef:       row.txn_ref,
                txnType:      <TxnType>row.txn_type,
                amount:       row.amount,
                description:  row.description,
                balanceAfter: row.balance_after,
                txnDate:      row.txn_date
            };

        if txnsResult is error {
            return <http:InternalServerError>{
                body: <ErrorResponse>{code: "DB_ERROR", message: txnsResult.message()}
            };
        }

        return {
            accountNumber:  accountRow.account_number,
            accountName:    accountRow.account_name,
            currentBalance: accountRow.balance,
            currency:       "NGN",
            transactions:   txnsResult,
            totalRecords:   txnsResult.length()
        };
    }
}
