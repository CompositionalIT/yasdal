# Yadsl
## Yet Another (F#) SQL Data Access Layer

Currently just an `fsx` file that you can copy. It wraps the following dependencies:

* Dapper
* TaskBuilder.fs
* Microsoft.Data.SqlClient

It has a small API set of methods, predicated on the following:

> **F# types do not naturally lend themselves to direct SQL table mappings.**
>
> **Therefore most ORMs and Micro ORMs are inappropriate for F#** e.g. Entity Framework or Dapper.

This leaves two real choices:

### Option 1: Intermediate type layer
1. Read from the database to an intermediary type that maps 1:1 with the database (you can use an ORM for this)
1. Then, map from that type to your rich F# domain model.

### Option 2: Map directly from SQL result sets.
This is the approach taken by Yasdal. You map directly from a lightweight abstraction that sits on top of an `IDataReader` and manually extract values from the SQL data reader into your F# types, using whatever logic you require.

## Reading Data
Yasdal has a small surface area for reading:

### Fetch (reading a collection of records)

```SQL
SELECT * FROM Production.Product
```

| Name | ProductNumber | MakeFlag | Color |
|-|-|-|-|
| Blade	| BL-2036	| 1	| NULL |
| LL Crankarm	| CA-5965	| 0	| Black |
| ML Crankarm	| CA-6738	| 0	| Black |
| HL Crankarm	| CA-7457	| 0	| Black |

```fsharp
open Yasdal

type ProductNumber = ProductNumber of string
type Color = Color of string

let results = // Task<Product array>
    Db.Fetch(
        connectionString,
        "SELECT * FROM Production.Product",
        fun read ->
            {
                Name = read.string "Name"
                ProductNumber = read.string "ProductNumber"
                Flag = read.bool "MakeFlag"
                Color = read.stringOption "Color" |> Option.map Color
            }
    )
```

### FetchOne
As per Fetch, except returns an optional single result.

### FetchMany
If you require multiple result sets to create your data, use `FetchMany` to execute multiple queries and return them.

```fsharp
let productsAndPrices = // Task<Product array * PriceHistory array>
    Db.FetchMany (
        connectionString,
        ResultSet.create ("SELECT * FROM Production.Product", Mappers.toProduct), // mapping code elided
        ResultSet.create ("SELECT pch.* FROM Production.ProductCostHistory pch JOIN Production.Product p on p.ProductID = pch.ProductID", Mappers.toPrice)  // mapping code elided
    )
```

In code, you should then construct your F# records by e.g. joining the data together as required.

> There are overloads for 2, 3 and 4 result sets.

#### Parameterisation
Since Yasdal is just a wrapper on top of Dapper, you can use the same technique for parameterisation:

```fsharp
Db.Fetch(
        connectionString,
        "SELECT * FROM Production.Product WHERE SafetyStockLevel = @SafetyStockLevel",
        Mappers.toProduct,
        {| SafetyStockLevel = 800 |}
    )
```

### Writing Data
Yasdal has support for a bulk insert wrapper. It takes a similar approach to writing as reading i.e. manually map between your F# rich types and the low level SQL columns. It uses a lightweight wrapper around `IDataReader` directly over your source data so you avoid the need for a `DataTable`, thus reducing memory and improving performance.

```fsharp
open Yasdal
open Yasdal.BulkInsert

Db.BulkInsert(
    connectionString,
    "Production.Product", // table to insert into
    products, // Product array - the data that you want to insert
    [
        Column.int ("ProductID", fun p -> p.ProductId)
        Column.string ("Name", fun p -> p.Name)
        Column.stringOpt ("Color", fun p -> p.Color |> Option.map (fun (Color c) -> c))
        Column.bool ("MakeFlag", fun p -> p.Make)
        Column.bool ("FinishedGoodsFlag", fun p -> p.FinishedGoods)
        Column.stringOpt ("Size", fun p -> p.Size |> Option.map string)
        Column.string ("ProductNumber", fun p -> p.ProductNumber )
    ]
)
```

The `Column` module contains functions for working with the standard set of primitives, but a generic `make` method also exists that will infer the column type based on the result of the lambda expression (use with care).

### Error Handling
Yasal doesn't use Results (yet?). Instead, there are a few custom exceptions that wrap around low level ADO .NET exceptions for incorrect mappings etc.