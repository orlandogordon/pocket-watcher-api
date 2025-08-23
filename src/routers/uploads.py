from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, BackgroundTasks, Form
from sqlalchemy.orm import Session
import uuid
from typing import Optional

from src.db.core import get_db
# This is a placeholder for a proper authentication dependency.
# In a real app, this would decode a JWT token to get the current user.
from src.routers.accounts import get_current_user_id 
from src.services import s3, importer

router = APIRouter(
    prefix="/uploads",
    tags=["uploads"],
)

@router.post("/statement", status_code=status.HTTP_202_ACCEPTED)
async def upload_statement(
    background_tasks: BackgroundTasks,
    institution: str = Form(...),
    file: UploadFile = File(...),
    account_id: Optional[int] = Form(None),
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db)
):
    """
    Upload a financial statement file (PDF or CSV) for asynchronous processing.

    This endpoint accepts a file, uploads it to a secure storage bucket,
    and schedules a background task to parse the statement and import the data.

    - **institution**: The name of the financial institution (e.g., 'amex', 'tdbank').
    - **file**: The statement file to upload.
    - **account_id**: (Optional) The ID of the account to associate with all transactions from this file.
    """
    if file.content_type not in ["application/pdf", "text/csv"]:
        raise HTTPException(status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, detail="Only PDF or CSV files are supported.")

    # Generate a unique, secure filename
    file_extension = ".pdf" if file.content_type == "application/pdf" else ".csv"
    unique_filename = f"{uuid.uuid4()}{file_extension}"
    s3_key = f"statements/{user_id}/{unique_filename}"

    try:
        # Upload the file to S3
        s3.upload_file_to_s3(file_obj=file.file, bucket=s3.get_s3_bucket(), object_name=s3_key)

        # Add the processing job to the background
        background_tasks.add_task(
            importer.process_statement, 
            db=db,
            user_id=user_id, 
            s3_key=s3_key, 
            institution=institution,
            file_content_type=file.content_type,
            account_id=account_id
        )

        return {"message": "File upload accepted and is being processed.", "s3_key": s3_key}

    except Exception as e:
        # In a real app, you'd have more specific error handling and logging
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An error occurred during file upload: {e}")
