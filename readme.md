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

- uvicorn src.main:app --reload

- Run Claude Code: npx @anthropic-ai/claude-code


## Bulk Upload Script instruction
  Before you run it, you need to:

   1. Edit `scripts/bulk_upload.py` and update the ACCOUNT_MAPPING dictionary with the correct
      account_id for each folder. I've put in placeholder values.
   2. Start your FastAPI server in a separate terminal with the command: uvicorn src.main:app 
      --reload
   3. Run the script with: python scripts/bulk_upload.py

  The script will then go through your input folder and upload the files.

  A note on authentication: The script currently doesn't send any authentication headers. If
  your /uploads/ endpoint is protected, you'll need to add an Authorization header in the
  HEADERS dictionary within the script.



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