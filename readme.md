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


## Architectural Notes

- The application follows a standard Next.js App Router structure.
- Reusable UI components are located in `src/components`.
- Firebase configuration and utility functions are in `src/lib/firebase.ts`.
- Global styles and Tailwind CSS configuration are in `src/app/globals.css` and `tailwind.config.ts` respectively.
- The application is a Progressive Web App (PWA), with configuration in `public/manifest.json`.
- Firebase Firestore rules are available in `firebaserules.txt` file.

## Implementation standard.

- DO NOT over engineer things. Start with the simplest implementation.
- Always keep the performance and security as a first priority.
- Ask for any clarification rather just guessing things if you are not clear about anything.