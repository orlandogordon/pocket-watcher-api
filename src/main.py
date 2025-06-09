from fastapi import FastAPI, APIRouter
from .routers.users import router as users_router
from .routers.transactions import router as transactions_router
from .routers.investments import router as investments_router

# @asynccontextmanager
# async def lifespan(_: FastAPI):
#     yield

test_router = APIRouter()


app = FastAPI()  # FastAPI(lifespan=lifespan)


@test_router.get("/posts")
async def posts():
    return {"posts": "test"}


app.include_router(users_router)
app.include_router(transactions_router)
app.include_router(investments_router)
app.include_router(test_router)

# app.state.limiter = limiter
# app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)



@app.get("/")
def read_root():
    return "Server is running."



# from fastapi import FastAPI, HTTPException, Query, Path
# from pydantic import BaseModel, ConfigDict
# from pathlib import Path
# from sqlalchemy import Boolean, Column, Integer, String, DateTime, create_engine
# from sqlalchemy.orm import declarative_base
# from sqlalchemy.orm import Session
# from typing import List, Optional, Generic, TypeVar
# from datetime import datetime
# # from . import parser

# # Establish a connection to the database
# DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/pocket_watcher_db"
# engine = create_engine(DATABASE_URL)
# session = Session(engine)
# # Define the base model class
# Base = declarative_base()
# # Define the User model
# # class UserOrm(Base):
# #   __tablename__ = "users"

# #   id = Column(Integer, primary_key=True, index=True, autoincrement=True)
# #   name = Column(String, index=True)
# #   email = Column(String, unique=True, index=True)
# #   is_active = Column(Boolean, default=True)

# class BlogDB(Base):
#     __tablename__ = "blogs"

#     id = Column(Integer, primary_key=True, index=True, autoincrement=True)
#     title = Column(String)
#     content = Column(String)
#     created_at = Column(DateTime, default=datetime.now())

# # Create the table
# Base.metadata.create_all(bind=engine)

# # Initialize FastAPI app
# app = FastAPI()

# class BlogBase(BaseModel):
#     title: str
#     content: str

# class BlogCreate(BlogBase):
#     pass  # No additional fields needed

# class BlogUpdate(BlogBase):
#     title: str | None = None  # Allow partial updates
#     content: str | None = None

# class BlogResponse(BlogBase):
#     id: int
#     created_at: datetime

#     model_config = ConfigDict(from_attributes=True)

# # Routes
# @app.get("/")
# def homepage():
#  return {"message": "Welcome to the homepage"}

# @app.post("/blogs/", response_model=BlogResponse)
# async def create_blog(blog: BlogCreate):
#     # Simulate saving to DB
#     db_blog = BlogDB(**blog.model_dump())
#     session.add(db_blog)
#     session.commit()
#     session.refresh(db_blog)
#     return db_blog

# @app.patch("/blogs/{blog_id}", response_model=BlogResponse)
# async def update_blog(blog_id: int, blog: BlogUpdate):
#     blog = session.query(BlogDB).filter(BlogDB.id == blog_id).first()
#     if blog is None:
#       raise HTTPException(status_code=404, detail="User not found")
#     return blog

# @app.get("/blogs/", response_model=list[BlogResponse])
# async def get_blogs(category: str | None = Query(default=None)):
#     # Simulate fetching blogs by category
#     # blogs = session.query(BlogDB).offset(skip).limit(limit).all()
#     blogs = session.query(BlogDB).all()
#     return blogs

# @app.get("/blogs/{blog_id}", response_model=BlogResponse)
# async def get_blog(blog_id: int = Path(ge=1)):
#     blog = session.query(BlogDB).filter(BlogDB.id == blog_id).first()
#     if blog is None:
#       raise HTTPException(status_code=404, detail="User not found")
#     return blog

# # Simulate fetching a blog from the database
# blog = BlogCreate(title="My First Blog", content="This is the content of my first blog.")
# blog_db = BlogDB(**blog.model_dump())
# print(blog_db.__dict__)  # {'_sa_instance_state': <sqlalchemy.orm.state.InstanceState object at ...>, 'title': 'My First Blog', 'content': 'This is the content of my first blog.'}
# # db_blog = BlogDB(id=1, title="ORM Post", content="Content", created_at=datetime.now())
# pydantic_blog = BlogBase.model_validate(blog_db.__dict__)  # V1: orm_mode=True
# print(pydantic_blog.model_dump())  # {'id': 1, 'title': 'ORM Post', ...}
# Pydantic models
# class UserSchema(BaseModel):
#   name: str
#   email: str
#   is_active: bool = True

# class UserCreate(UserBase):
#   pass

# class User(UserBase):
#   model_config = ConfigDict(from_attributes=True)
  
#   id: int | None


# @app.post("/users/", response_model=UserSchema)
# def create_user(user: UserSchema):
#   db_user = UserOrm(**user.model_dump())
#   session.add(db_user)
#   session.commit()
#   session.refresh(db_user)
#   return db_user

# @app.get("/users/", response_model=List[UserSchema])
# def read_users(skip: int = 0, limit: int = 100):
#   users = session.query(UserOrm).offset(skip).limit(limit).all()
#   return users

# @app.get("/users/{user_id}", response_model=UserSchema)
# def read_user(user_id: int):
#   user = session.query(UserOrm).filter(UserSchema.id == user_id).first()
#   if user is None:
#     raise HTTPException(status_code=404, detail="User not found")
#   return user

# @app.put("/users/{user_id}", response_model=UserSchema)
# def update_user(user_id: int, user: UserSchema):
#   db_user = session.query(UserSchema).filter(UserSchema.id == user_id).first()
#   if db_user is None:
#     raise HTTPException(status_code=404, detail="User not found")
#   for key, value in user.dict().items():
#     setattr(db_user, key, value)
#   session.commit()
#   session.refresh(db_user)
#   return db_user

# @app.delete("/users/{user_id}", response_model=UserSchema)
# def delete_user(user_id: int):
#   user = session.query(UserOrm).filter(UserSchema.id == user_id).first()
#   if user is None:
#     raise HTTPException(status_code=404, detail="User not found")
#   session.delete(user)
#   session.commit()
#   return user

# def main():
#     # sys.path.append(str(Path(__file__).parent))
#     root_dir=Path(__file__).parent.parent

#     parser.ParserProcess()
    
# if __name__ == "__main__":
#     main()