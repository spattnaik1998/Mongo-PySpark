--Querying Data in MongoDB
match operator: 
db.Persons.aggregate([
    {
        $match: {
            age: { $gt: 18 }
        }
    }
]);

db.Persons.aggregate([{$match: {gender: 'male'}}])

{
    $group: {
        _id: <expression>, // Grouping expression
        <field1>: { <accumulator1>: <expression1> },
        <field2>: { <accumulator2>: <expression2> },
        // ...
    }
}
db.sales.aggregate([
    {
        $group: {
            _id: "$product",
            totalSales: { $sum: "$quantity" },
            averagePrice: { $avg: "$price" },
            minPrice: { $min: "$price" },
            maxPrice: { $max: "$price" }
        }
    }
]);

db.Persons.aggregate([{$group: {_id: {age: "$age", gender: "$gender"}}}])

dbPersons.aggregrate([{$group: {_id: "$company"}}])

db.Persons.aggregate([{$group: {_id: {age: "$age", gender: "$gender", isActive: "$isActive"}}}])

db.Persons.aggregate([
// Stage 1
{$match: {gender: "female"}},
// Stage 2
{$group: {_id: {eyeColor: "$eyeColor", age: "$age", gender: "$gender"}}}
])

db.Persons.aggregate([
//Stage 1
{$group: {_id: {age: "$age", eyeColor: "$eyeColor"}}},
{$match: {"_id.age": {$gt: 30}}}
])

db.Persons.aggregate([{$count: "total"}])

db.Persons.aggregate([{
//Stage 1
$group: {_id: {eyeColor: "$eyeColor", gender: "$gender"}}},
//Stage 2
{$count: "eyeColorAndGender"}
])

db.Persons.aggregate([{
//Stage 1
$match: {age: {$gt: 25}}},
//Stage 2
{$group: {_id: {eyeColor: "$eyeColor", gender: "$gender"}}},
//Stage 3
{$count: "eyeColorAndGender"}
])

db.Persons.aggregate([
{$sort: {age: -1, gender: -1}}
])

db.Persons.aggregate([

{$group: {_id: "$age"}},

{$sort: {_id: 1}}
])

db.Persons.aggregate([
{$match: {eyeColor: {$ne: "blue"}}},
{$group: {_id: {eyeColor: "$eyeColor", favoriteFruit: "$favoriteFruit"}}},
{$sort: {"_id.eyeColor": 1, "_id.favoriteFruit": -1}}
])

db.Persons.aggregate([{$project: {isActive: 0, name: 0, gender: 0}}])

db.Persons.aggregate([
{$limit: 100},
{$match: {age: {$gt: 27}}},
{$group: {_id: "$company.location.country"}}
])

db.Persons.aggregate([
{$limit:100},
{$match: {eyeColor: {$ne: "blue"}}},
{$group: {_id: {eyeColor: "$eyeColor", favoriteFruit: "$favoriteFruit"}}},
{$sort: {"_id.eyeColor": 1, "_id.favoriteFruit": -1}}
])

db.Persons.aggregate([
{$unwind: "$tags"},
{$group: {_id: "$tags"}}
])

db.Persons.aggregate([
{$group: {
	_id: "$favoriteFruit",
	count: {$sum: 1}
}}
])

db.Persons.aggregate([
	{$unwind: "$tags"},
	{$group: {
		_id: "$tags",
		count: {$sum: NumberInt(1)}
	}}
])

db.Persons.aggregate([
	{$unwind: "$tags"},
	{$group: {
		_id: "$company.location.country",
		avgAge: {$avg: "$age"}
	}}
])