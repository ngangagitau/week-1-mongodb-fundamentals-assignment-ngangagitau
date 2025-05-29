// queries.js
const { MongoClient } = require('mongodb');

const uri = 'mongodb://localhost:27017'; // Change if using Atlas
const dbName = 'plp_bookstore';
const collectionName = 'books';

async function run() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected to MongoDB');

    const db = client.db(dbName);
    const books = db.collection(collectionName);

    // --- Task 2: Basic CRUD Operations ---

    // Find all books in a specific genre, e.g., 'Fiction'
    const fictionBooks = await books.find({ genre: 'Fiction' }).toArray();
    console.log('\nFiction Books:', fictionBooks);

    // Find books published after 2000
    const recentBooks = await books.find({ published_year: { $gt: 2000 } }).toArray();
    console.log('\nBooks published after 2000:', recentBooks);

    // Find books by a specific author, e.g., 'George Orwell'
    const orwellBooks = await books.find({ author: 'George Orwell' }).toArray();
    console.log('\nBooks by George Orwell:', orwellBooks);

    // Update the price of 'The Hobbit' to 16.99
    const updateResult = await books.updateOne(
      { title: 'The Hobbit' },
      { $set: { price: 16.99 } }
    );
    console.log(`\nUpdated 'The Hobbit' price: ${updateResult.modifiedCount} document(s) modified`);

    // Delete a book by title, e.g., 'Moby Dick'
    const deleteResult = await books.deleteOne({ title: 'Moby Dick' });
    console.log(`\nDeleted books titled 'Moby Dick': ${deleteResult.deletedCount}`);

    // --- Task 3: Advanced Queries ---

    // Find books that are in stock AND published after 2010
    const inStockRecent = await books.find({
      in_stock: true,
      published_year: { $gt: 2010 }
    }).toArray();
    console.log('\nIn-stock books published after 2010:', inStockRecent);

    // Projection: return only title, author, and price fields
    const projectedBooks = await books.find({}, {
      projection: { title: 1, author: 1, price: 1, _id: 0 }
    }).toArray();
    console.log('\nBooks with only title, author, and price:', projectedBooks);

    // Sorting by price ascending
    const sortedAsc = await books.find().sort({ price: 1 }).toArray();
    console.log('\nBooks sorted by price ascending:', sortedAsc);

    // Sorting by price descending
    const sortedDesc = await books.find().sort({ price: -1 }).toArray();
    console.log('\nBooks sorted by price descending:', sortedDesc);

    // Pagination: 5 books per page, page 1 (skip 0)
    const page1 = await books.find()
      .skip(0)
      .limit(5)
      .toArray();
    console.log('\nPage 1 (5 books):', page1);

    // Pagination: page 2 (skip 5)
    const page2 = await books.find()
      .skip(5)
      .limit(5)
      .toArray();
    console.log('\nPage 2 (5 books):', page2);

    // --- Task 4: Aggregation Pipeline ---

    // Average price of books by genre
    const avgPriceByGenre = await books.aggregate([
      {
        $group: {
          _id: "$genre",
          averagePrice: { $avg: "$price" },
          count: { $sum: 1 }
        }
      },
      { $sort: { averagePrice: -1 } }
    ]).toArray();
    console.log('\nAverage price by genre:', avgPriceByGenre);

    // Author with the most books
    const topAuthor = await books.aggregate([
      {
        $group: {
          _id: "$author",
          bookCount: { $sum: 1 }
        }
      },
      { $sort: { bookCount: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log('\nAuthor with the most books:', topAuthor);

    // Group books by publication decade and count
    const booksByDecade = await books.aggregate([
      {
        $project: {
          decade: {
            $multiply: [
              { $floor: { $divide: ["$published_year", 10] } },
              10
            ]
          }
        }
      },
      {
        $group: {
          _id: "$decade",
          count: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } }
    ]).toArray();
    console.log('\nBooks grouped by publication decade:', booksByDecade);

    // --- Task 5: Indexing ---

    // Create an index on the title field
    const titleIndexName = await books.createIndex({ title: 1 });
    console.log(`\nCreated index on title: ${titleIndexName}`);

    // Create a compound index on author and published_year
    const compoundIndexName = await books.createIndex({ author: 1, published_year: -1 });
    console.log(`Created compound index on author and published_year: ${compoundIndexName}`);

    // Use explain() to show performance of a query before and after index (example)
    // Let's query for a book by title using explain

    const explainWithoutIndex = await books.find({ title: "The Hobbit" }).explain("executionStats");
    console.log('\nExplain for query on title "The Hobbit":');
    console.log(JSON.stringify(explainWithoutIndex.executionStats, null, 2));

  } catch (err) {
    console.error(err);
  } finally {
    await client.close();
    console.log('Disconnected from MongoDB');
  }
}

run();
