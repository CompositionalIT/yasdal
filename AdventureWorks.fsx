#load "Yasdal.fsx"

open System
open Yasdal

[<AutoOpen>]
module Domain =
    // Nested record
    type ProductNumber =
        {   Family : string
            Code : string
            SubCode : string option }

    // Single case DU
    type Color = Color of string

    type Price = { Start : DateTime; End : DateTime option; Cost : decimal }
    type Product =
        {   ProductId : int
            Name : string
            ProductNumber : ProductNumber
            Make : bool
            FinishedGoods : bool
            Color : Color option
            Size : char option }

module Mappers =
    open Domain
    let toProduct (r:DataReader) =
        {
            ProductId = r.int "ProductID"
            Name = r.string "Name"
            ProductNumber =
                let raw = r.string "ProductNumber" |> fun s -> s.Split '-'
                match raw with
                | [| family; code |] -> { Family = family; Code = code; SubCode = None }
                | [| family; code; subCode |] -> { Family = family; Code = code; SubCode = Some subCode }
                | c -> failwith $"Unknown code '{c}'"

            Make = r.bool "MakeFlag"
            FinishedGoods = r.bool "FinishedGoodsFlag"
            Color = r.stringOption "Color" |> Option.map Color
            Size = r.charOption "Size"
        }

    let toPrice (r:DataReader) =
        {|
            ProductId = r.int "ProductId"
            Start = r.date "StartDate"
            End = r.dateOption "EndDate"
            Cost = r.decimal "StandardCost"
        |}

let connectionString = @"Server=(localdb)\MSSQLLocalDB;Database=AdventureWorks"

// Basic queries for single fields
let listOfStrings = Db.Fetch (connectionString, "SELECT DISTINCT NAME FROM Production.Product", fun r -> r.string "Name")
let listOfInts = Db.Fetch (connectionString, "SELECT DISTINCT ProductId FROM Production.Product", fun r -> r.int "ProductId")

// Basic query of a full object
let productsAlone = Db.Fetch (connectionString, "SELECT * FROM Production.Product", Mappers.toProduct)

// Query of a single row, parameterised
let singleProduct productId =
    Db.FetchOne (connectionString, "SELECT TOP (1) * FROM Production.Product WHERE ProductID = @ProductId", Mappers.toProduct, {| ProductId = productId |})

// Query of a multi result set, with an in-memory join using a query expression, using a parameter.
let productWithHistory chars =
    let parameters = [| "chars", chars+"%" |]
    Db.FetchMany (
        connectionString,
        ResultSet.create ("SELECT * FROM Production.Product WHERE ProductNumber LIKE @chars", Mappers.toProduct, parameters),
        ResultSet.create ("SELECT pch.* FROM Production.ProductCostHistory pch JOIN Production.Product p on p.ProductID = pch.ProductID WHERE ProductNumber LIKE @chars", Mappers.toPrice, parameters)
    )

let products, prices = (productWithHistory "LJ").Result

let joined =
    query {
        for product in products do
        groupJoin price in prices on (product.ProductId = price.ProductId) into prices
        select (
            {|
                Product = product
                Prices = [|
                    for price in prices do
                        {
                            Start = price.Start
                            End = price.End
                            Cost = price.Cost
                        }
                |]
            |}
        )
    } |> Seq.toArray

open BulkInsert

// Inserting data with bulk insert - doesn't work because we've not provided all columns, but you should get the idea.
let insertResult =
    Db.BulkInsert(
        connectionString,
        "Production.Product",
        productsAlone.Result,
        [
            Column.int ("ProdductID", fun r -> r.ProductId)
            Column.string ("Name", fun r -> r.Name)
            Column.stringOpt ("Color", fun r -> r.Color |> Option.map (fun (Color c) -> c))
            Column.bool ("MakeFlag", fun r -> r.Make)
            Column.bool ("FinishedGoodsFlag", fun r -> r.FinishedGoods)
            Column.stringOpt ("Size", fun r -> r.Size |> Option.map string)
            Column.string ("ProductNumber", fun r -> [ r.ProductNumber.Code; r.ProductNumber.Family; yield! Option.toList r.ProductNumber.SubCode ] |> String.concat "-" )
        ]
    )