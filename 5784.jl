using CSV, DataFrames, DataFramesMeta, Dates

noahs_customers = CSV.File("../5784/noahs-customers.csv") |> DataFrame
noahs_orders = CSV.File("../5784/noahs-orders.csv") |> DataFrame
noahs_orders_items = CSV.File("../5784/noahs-orders_items.csv") |> DataFrame
noahs_products = CSV.File("../5784/noahs-products.csv") |> DataFrame

### Day 1

function replace_chars(input_string)
    letters = join(Char.(97:122))
    mapping = Dict(zip(letters, ["2", "2", "2", "3", "3", "3", "4", "4", "4", "5", "5", "5", "6", "6", "6", "7", "7", "7", "7", "8", "8", "8", "9", "9", "9", "9"]))
    for (k, v) in mapping
        input_string = replace(lowercase(input_string), k => v)
    end
    return input_string
end

@chain noahs_customers begin
    transform(:name => ByRow(x -> join(split(x, " ")[2:end], " ")) => :last_name)
    transform(:last_name => ByRow(x -> replace(x, " III" => "", " IV" => "", " Jr." => "")) => :last_name)
    transform(:last_name => ByRow(x -> replace_chars(x)) => :last_name_transformed)
    transform(:phone => ByRow(x -> replace(x, "-" => "")) => :phone)
    filter(row -> row.phone == row.last_name_transformed, _)
end
# Sam Tannenbaum 826-636-2286

### Day 2

@chain noahs_customers begin
    leftjoin(noahs_orders, on=:customerid, matchmissing=:equal)
    leftjoin(noahs_orders_items, on=:orderid, matchmissing=:equal)
    leftjoin(noahs_products, on=:sku, matchmissing=:equal)
    transform(:name => ByRow(x -> join(split(x, " ")[2:end], " ")) => :last_name)
    transform(:name => ByRow(x -> join(split(x, " ")[1], " ")) => :first_name)
    transform(:last_name => ByRow(x -> replace(x, " III" => "", " IV" => "", " Jr." => "")) => :last_name)
    @rsubset occursin.("Bagel", coalesce.(:desc, ""))
    transform(:ordered => ByRow(x -> DateTime(x, dateformat"yyyy-mm-dd HH:MM:SS")) => :ordered)
    filter(row -> startswith(row.first_name, "J") && startswith(row.last_name, "P") && year(row.ordered) == 2017, _)
end
# Joshua Peterson 332-274-4185

### Day 3

@chain noahs_customers begin
    @rsubset(year(:birthdate) in [1927, 1939, 1951, 1963, 1975, 1987, 1999, 2011, 2023])
    @rsubset(month(:birthdate) in [6, 7])
    @rsubset occursin.("Jamaica, NY 11435", :citystatezip)
end
# Robert Morton 917-288-9635

### Day 4

@chain noahs_customers begin
    leftjoin(noahs_orders, on=:customerid, matchmissing=:equal)
    leftjoin(noahs_orders_items, on=:orderid, matchmissing=:equal)
    leftjoin(noahs_products, on=:sku, matchmissing=:equal)
    @rsubset occursin.("BKY", coalesce.(:sku, ""))
    @rsubset(hour(DateTime(:ordered, "yyyy-mm-dd HH:MM:SS")) < 5)
    @rsubset(hour(DateTime(:shipped, "yyyy-mm-dd HH:MM:SS")) < 5)
    @rsubset(year(:birthdate) > 1970)
    @rsubset occursin.("Brooklyn", coalesce.(:citystatezip, "")) || occursin.("Manhattan", coalesce.(:citystatezip, ""))
end
# Renee Harmon 607-231-3605

### Day 5

@chain noahs_customers begin
    leftjoin(noahs_orders, on=:customerid, matchmissing=:equal)
    leftjoin(noahs_orders_items, on=:orderid, matchmissing=:equal)
    leftjoin(noahs_products, on=:sku, matchmissing=:equal)
    @rsubset occursin.("Cat", coalesce.(:desc, ""))
    @rsubset occursin.("Staten Island", coalesce.(:citystatezip, ""))
    @rsubset :qty >= 10
end
# Nicole Wilson 631-507-6048

### Day 6

@chain noahs_customers begin
    leftjoin(noahs_orders, on=:customerid, matchmissing=:equal)
    leftjoin(noahs_orders_items, on=:orderid, matchmissing=:equal)
    leftjoin(noahs_products, on=:sku, matchmissing=:equal)
    groupby(:orderid)
    @transform(:total_cost = sum(:wholesale_cost); ungroup=false)
    @transform(:profit = :total .- :total_cost; ungroup=false)
    @transform(:negative_profit_count = sum(:profit .< 0))
    @orderby -:negative_profit_count
end
# Sherri Long 585-838-9161

### Day 7

df = @chain noahs_customers begin
    leftjoin(noahs_orders, on=:customerid, matchmissing=:equal)
    leftjoin(noahs_orders_items, on=:orderid, matchmissing=:equal)
    leftjoin(noahs_products, on=:sku, matchmissing=:equal)
    @rsubset occursin.("COL", coalesce.(:sku, ""))
    transform(:ordered => ByRow(x -> DateTime(x, dateformat"yyyy-mm-dd HH:MM:SS")) => :ordered)
    # remove colors in parenthesis
    transform(:desc => ByRow(x -> replace(x, r"\(\w+\)" => "")) => :desc_clean)
end

sherri_items = @chain df begin
    subset(:name => ByRow(x -> x == "Sherri Long"))
    _.desc_clean
end

sherri_dates = @chain df begin
    subset(:name => ByRow(x -> x == "Sherri Long"))
    _.ordered
end

@chain df begin
    @rsubset(Date(:ordered) in Date.(sherri_dates))
    @rsubset(:desc_clean in sherri_items)
end
# Carlos Myers 838-335-7157

### Day 8

@chain noahs_customers begin
    leftjoin(noahs_orders, on=:customerid, matchmissing=:equal)
    leftjoin(noahs_orders_items, on=:orderid, matchmissing=:equal)
    leftjoin(noahs_products, on=:sku, matchmissing=:equal)
    @rsubset occursin.("COL", coalesce.(:sku, ""))
    groupby(:name)
    transform(:name => length)
    @orderby -:name_length
    first()
end
# James Smith 212-547-3518




















