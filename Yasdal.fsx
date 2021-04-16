#r "nuget: Microsoft.Data.SqlClient, 3.0.0-preview1.21075.2"
#r "nuget: Taskbuilder.fs"
#r "nuget: Dapper"

open Dapper
open System
open System.Data
open System.Data.SqlTypes
open Microsoft.Data.SqlClient
open FSharp.Control.Tasks.V2

module Exceptions =
    type ColumnMismatchException(columnName, inner:InvalidCastException) =
        inherit exn ($"Cast error for column '%s{columnName}'.", inner)
        member _.ColumnName = columnName
    type ColumnNullException(columnName, inner:SqlNullValueException) =
        inherit exn ($"Null value for column '%s{columnName}' when non-null expected.", inner)
        member _.ColumnName = columnName
    type UnknownColumnException(columnName, inner:IndexOutOfRangeException) =
        inherit exn ($"No such column '%s{columnName}' exists.", inner)
        member _.ColumnName = columnName

open Exceptions

type IDataReader with
    member inline private r.asMandatory mapper col =
        try
            r.GetOrdinal col |> mapper
        with
        | :? InvalidCastException as ex -> raise (ColumnMismatchException(col, ex))
        | :? IndexOutOfRangeException as ex -> raise (UnknownColumnException(col, ex))
        | :? SqlNullValueException as ex -> raise (ColumnNullException(col, ex))
    
    member inline private r.asOptional mapper col =
        try
            let index = r.GetOrdinal col
            if r.IsDBNull index then None else Some(mapper index)
        with
        | :? InvalidCastException as ex -> raise (ColumnMismatchException(col, ex))
        | :? IndexOutOfRangeException as ex -> raise (UnknownColumnException(col, ex))
        | :? SqlNullValueException as ex -> raise (ColumnNullException(col, ex))
    member r.char = r.asMandatory (r.GetString >> Seq.head)
    member r.charOption = r.asOptional (r.GetString >> Seq.head)
    member r.int = r.asMandatory r.GetInt32
    member r.intOption = r.asOptional r.GetInt32
    member r.int64 = r.asMandatory r.GetInt64
    member r.int64Option = r.asOptional r.GetInt64
    member r.decimal = r.asMandatory r.GetDecimal
    member r.decimalOption = r.asOptional r.GetDecimal
    member r.date = r.asMandatory r.GetDateTime
    member r.dateOption = r.asOptional r.GetDateTime
    member r.bool = r.asMandatory r.GetBoolean
    member r.boolOption = r.asOptional r.GetBoolean
    member r.float = r.asMandatory r.GetDouble
    member r.floatOption = r.asOptional r.GetDouble
    member r.guid = r.asMandatory r.GetGuid
    member r.guidOption = r.asOptional r.GetGuid
    member r.string = r.asMandatory r.GetString
    member r.stringOption = r.asOptional r.GetString

type private Common.DbDataReader with
    member this.MapResults mapper =
        let output = ResizeArray()
        while this.Read() do
            output.Add (mapper (this :> IDataReader))
        output
    member this.NextResultSet () = task {
        let! _ = this.NextResultAsync()
        return ()
    }

type ResultSet<'a, 'b> =
    {
        Query : string
        Parameters : (string * 'b) array
        Mapper : IDataReader -> 'a
    }
type ResultSet () =
    static member inline create (query, mapper, ?args:(string * #obj) array) = { Query = query; Parameters = defaultArg args [||]; Mapper = mapper }

type Db () =
    static member private connect connectionString = task {
        let connection = new SqlConnection(connectionString)
        do! connection.OpenAsync()
        return connection
    }

    static  member private executeReaderMany (conn:SqlConnection) queries parameters =
        let dedupedParams =
            parameters
            |> Array.map (fun (a,b) -> a, box b)
            |> Array.groupBy fst
            |> Array.map (fun (key, values) ->
                let firstValue = values.[0]
                if values |> Array.forall (fun v -> v = firstValue) then firstValue
                else failwith $"Different params found with the same key '{key}'.")
            |> dict
        let combinedQuery = String.concat ";\n\n" queries
        conn.ExecuteReaderAsync (combinedQuery, dedupedParams, commandTimeout = 600)

    /// Fetches a collection of rows from the database.
    static member Fetch (connection:SqlConnection, query, mapper, ?inputArgs:obj, ?timeout, ?transaction:SqlTransaction) = task {
        let timeout = defaultArg timeout 600
        let inputArgs = Option.toObj inputArgs
        let transaction = Option.toObj transaction
        use! reader = connection.ExecuteReaderAsync(query, param = inputArgs, transaction = transaction, commandTimeout = timeout)

        let output = ResizeArray()
        while reader.Read() do
            output.Add (mapper reader)
        return output
    }

    /// Fetches a collection of rows from the database.
    static member Fetch (connectionString, query, mapper, ?inputArgs:obj) = task {
        use! connection = Db.connect connectionString
        return! Db.Fetch (connection, query, mapper, ?inputArgs = inputArgs)
    }

    /// Tries to fetch a single row from the database.
    static member FetchOne (connectionString, query, mapper, ?inputArgs:obj) = task {
        use! connection = Db.connect connectionString
        use! r = connection.ExecuteReaderAsync(query, Option.toObj inputArgs)
        return
            if not (r.Read()) then None
            else Some (mapper r)
    }

    /// Fetches a collection of rows in two result sets from the database.
    static member FetchMany (connectionString, query1, query2) = task {
        use! connection = Db.connect connectionString
        let! reader = Db.executeReaderMany connection [| query1.Query; query2.Query |] ([| query1.Parameters; query2.Parameters |] |> Array.concat)
        let r1 = reader.MapResults query1.Mapper
        do! reader.NextResultSet ()
        let r2 = reader.MapResults query2.Mapper
        return r1, r2
    }

    /// Fetches a collection of rows in three result sets from the database.
    static member FetchMany (connectionString, query1, query2, query3) = task {
        use! connection = Db.connect connectionString
        let! reader = Db.executeReaderMany connection [| query1.Query; query2.Query; query3.Query|] ([| query1.Parameters; query2.Parameters; query3.Parameters |] |> Array.concat)
        let r1 = reader.MapResults query1.Mapper
        do! reader.NextResultSet ()
        let r2 = reader.MapResults query2.Mapper
        do! reader.NextResultSet ()
        let r3 = reader.MapResults query3.Mapper
        return r1, r2, r3
    }

    /// Fetches a collection of rows in four result sets from the database.
    static member FetchMany (connectionString, query1, query2, query3, query4) = task {
        use! connection = Db.connect connectionString
        let! reader = Db.executeReaderMany connection [| query1.Query; query2.Query; query3.Query; query4.Query|] ([| query1.Parameters; query2.Parameters; query3.Parameters; query4.Parameters |] |> Array.concat)
        let r1 = reader.MapResults query1.Mapper
        do! reader.NextResultSet ()
        let r2 = reader.MapResults query2.Mapper
        do! reader.NextResultSet ()
        let r3 = reader.MapResults query3.Mapper
        do! reader.NextResultSet ()
        let r4 = reader.MapResults query4.Mapper
        return r1, r2, r3, r4
    }

module BulkInsert =
    let inline internal buildIDataReader<'a> (rows:'a array) columns getValue =
        let fields = columns |> Array.mapi (fun index value -> value, index) |> readOnlyDict
        let mutable counter = -1
        { new IDataReader with
            member _.Depth = raise (NotImplementedException())
            member _.GetBoolean _ = raise (NotImplementedException())
            member _.GetByte _ = raise (NotImplementedException())
            member _.GetBytes (_, _, _, _, _) = raise (NotImplementedException())
            member _.GetChar _ = raise (NotImplementedException())
            member _.GetChars (_, _, _, _, _) = raise (NotImplementedException())
            member _.GetData _ = raise (NotImplementedException())
            member _.GetDataTypeName _ = raise (NotImplementedException())
            member _.GetDateTime _ = raise (NotImplementedException())
            member _.GetDecimal _ = raise (NotImplementedException())
            member _.GetDouble _ = raise (NotImplementedException())
            member _.GetFieldType _ = raise (NotImplementedException())
            member _.GetFloat _ = raise (NotImplementedException())
            member _.GetGuid _ = raise (NotImplementedException())
            member _.GetInt16 _ = raise (NotImplementedException())
            member _.GetInt32 _ = raise (NotImplementedException())
            member _.GetInt64 _ = raise (NotImplementedException())
            member _.GetName _ = raise (NotImplementedException())
            member _.GetSchemaTable () = raise (NotImplementedException())
            member _.GetString _ = raise (NotImplementedException())
            member _.GetValues _ = raise (NotImplementedException())
            member _.IsClosed = raise (NotImplementedException())
            member _.IsDBNull _ = raise (NotImplementedException())
            member _.Item with get (_: int) : obj = raise (NotImplementedException())
            member _.Item with get (_: string) : obj = raise (NotImplementedException())
            member _.NextResult() = raise (NotImplementedException())
            member _.RecordsAffected = raise (NotImplementedException())

            member _.Close () = ()
            member _.Dispose () = ()
            member _.FieldCount = fields.Count
            member _.GetOrdinal (name) = fields.[name]
            member _.GetValue index = getValue rows.[counter] index
            member _.Read () =
                counter <- counter + 1
                counter < rows.Length
        }

    /// Represents the specification for the name and type of a DB column, plus a function that can retrieve the data for that column given a row.
    type ColSpec<'RowType> = string * ('RowType -> obj)

    type Column () =
        /// Builds a raw mapping field. Prefer specialised builder functions if possible.
        static member make<'RowType, 'FieldType> (columnName:string, getValue:'RowType -> 'FieldType) : ColSpec<'RowType> = columnName, (fun (row:'RowType) -> box (getValue row))
        static member guid<'RowType> (columnName, getValue) = Column.make<'RowType, Guid> (columnName, getValue)
        static member guidOpt<'RowType> (columnName, getValue) = Column.make<'RowType, Guid Nullable> (columnName, getValue >> Option.toNullable)
        static member string<'RowType> (columnName, getValue) = Column.make<'RowType, string> (columnName, getValue)
        static member stringOpt<'RowType> (columnName, getValue) = Column.make<'RowType, string> (columnName, getValue >> Option.toObj)
        static member dateTime<'RowType> (columnName, getValue) = Column.make<'RowType, DateTime> (columnName, getValue)
        static member dateTimeOpt<'RowType> (columnName, getValue) = Column.make<'RowType, DateTime Nullable> (columnName, getValue >> Option.toNullable)
        static member int<'RowType> (columnName, getValue) = Column.make<'RowType, int> (columnName, getValue)
        static member intOpt<'RowType> (columnName, getValue) = Column.make<'RowType, int Nullable> (columnName, getValue >> Option.toNullable)
        static member bool<'RowType> (columnName, getValue) = Column.make<'RowType, bool> (columnName, getValue)
        static member boolOpt<'RowType> (columnName, getValue) = Column.make<'RowType, bool Nullable> (columnName, getValue >> Option.toNullable)
        static member decimal<'RowType> (columnName, getValue) = Column.make<'RowType, decimal> (columnName, getValue)
        static member decimalOpt<'RowType> (columnName, getValue) = Column.make<'RowType, decimal Nullable> (columnName, getValue >> Option.toNullable)

open BulkInsert

type Db with
    /// Mandatory use of transactions during bulk insert
    static member BulkInsert<'RowType> (connection, tableName, data, columns:'RowType ColSpec list, ?transaction:SqlTransaction) =
        let columns = columns |> List.toArray
        use bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, Option.toObj transaction)
        bulkCopy.DestinationTableName <- tableName
        bulkCopy.BulkCopyTimeout <- 0

        for (column, _) in columns do
            bulkCopy.ColumnMappings.Add (column, column) |> ignore

        let getValue row index =
            let _, getValue = columns.[index]
            getValue row

        let dataReader = buildIDataReader data (Array.map fst columns) getValue
        bulkCopy.WriteToServerAsync dataReader
    static member BulkInsert<'RowType> (connectionString, tableName, data, columns:'RowType ColSpec list) : Threading.Tasks.Task =
        task {
            use! connection = Db.connect connectionString
            do! Db.BulkInsert<'RowType> (connection, tableName, data, columns)
        } :> _
