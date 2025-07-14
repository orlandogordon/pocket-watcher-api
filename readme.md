## Other products to consider
- plain text accountng
- bean count
- hledger
- docuclipper
- streamlit
- time shift data

## TODO
- Breakout the image handling into a separate function
- Update the image handling logic to use output folder instead of png
- Consider class based approach to combine all parsers
- Implement more regex

## API Curl requests testing

uvicorn src.main:app --reload

curl -X GET http://127.0.0.1:8000/

curl -X POST -H "Content-Type: application/json" -d '{"title":"test blog1", "content":"This is the first comment in my test blog"}' 'http://127.0.0.1:8000/blogs'

curl -X GET 'http://127.0.0.1:8000/blogs/'

curl -X GET 'http://127.0.0.1:8000/blogs/2'

curl -X GET http://127.0.0.1:8000/items/0


## TODO
- Add user id validation wherever it is mapped as a foreign key
- Fix user table to use db_id - id pattern
- Refine model validation, optional attributes in pydantic models
- Create update and delete logic
- Streamlined testing setup? (pre written bash script maybe?)
- Add model factory to UserCreate pydantic model id field (see transactionCreate)