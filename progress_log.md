# 2025-08-25: De-duplication Logic Explanation

### Question: Why was I getting a `UNIQUE constraint` error when importing transactions to an empty database table, and why is it not better to just let the database schema reject duplicates rather than have application logic to check for them?

This question arose after implementing a fix in `src/crud/crud_transaction.py` to prevent duplicate transactions from being created during bulk import.

---

### Part 1: Why the Error Occurred on an Empty Table

The reason you saw the `UNIQUE constraint` error even with an empty table is that the duplicates were **within the single batch of transactions you were trying to import.**

Here's the sequence of events without the `processed_hashes` fix:

1.  Your code receives a list of transactions, e.g., `[tx_A, tx_B, tx_A]`.
2.  It loops through them. The first `tx_A` is processed and staged to be saved (`db.add(db_transaction)`).
3.  `tx_B` is processed and staged to be saved.
4.  The second `tx_A` is also processed and staged to be saved.
5.  At the end of the function, `db.commit()` is called. This tells the database to save all the staged transactions at once.
6.  The database tries to insert the first `tx_A` (success), then `tx_B` (success), then the second `tx_A`. When it tries to insert the second `tx_A`, it violates its own `UNIQUE` rule, throws an error, and the **entire transaction is rolled back**.

So, the error wasn't from a pre-existing item in the database, but from the code trying to insert two identical items in the same operation.

---

### Part 2: Why Not Let the Database Reject Duplicates?

While you *can* rely solely on the database schema to enforce uniqueness, it's generally considered best practice to handle it in the application code first for several key reasons:

1.  **Performance:** Checking if an item is in a Python `set` is extremely fast. Hitting the database, attempting an insert, having the database reject it, raising an exception, and catching that exception is a very "expensive" and slow operation in comparison. For a large import with many duplicates, this would significantly slow down the process.

2.  **Transactional Integrity (The "All-or-Nothing" Problem):** As described above, a single duplicate in a batch of thousands of transactions would cause the `db.commit()` to fail, rolling back the *entire batch*. This means all the valid, non-duplicate transactions would also fail to be saved. The current logic gracefully skips the duplicates and ensures all valid ones are successfully imported.

3.  **Clearer Error Handling and Logging:** By checking for duplicates in the code, you can control the outcome. You can log that a duplicate was found and skipped, or even return that information to the user. If you just let the database fail, you get a generic `IntegrityError`, and it's much harder to provide specific feedback about *why* the import failed.

In short, while the database constraint is the ultimate safety net, the application-level check is a crucial optimization that makes the import process more **robust, performant, and user-friendly.**

---

## To-Do

- Test, debug, and fix the remaining statement and CSV parsers to ensure they conform to the new `ParsedData` model and handle various file formats correctly.
- In the transactions table: We have a uuid where transaction hash is but I thought this would be the unique idenitifier with identifying data points like description date and amount. We also probably don't need the posted_date, raw_data_json and needs review columns. We also are uploading the parsed_description only to the description table when it should be in the parsed_description table and MAYBE in the description table too. 
