from pydantic import BaseModel, EmailStr, Field

class UserRegisterSchema(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8)

class UserLoginSchema(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8)

class UserLogoutSchema(BaseModel):
    refresh: str

class UserActivateSchema(BaseModel):
    email: EmailStr
    token: str

class UserResetPasswordSchema(BaseModel):
    email: EmailStr

class UserPasswordResetSchema(BaseModel):
    email: EmailStr
    token: str
    new_password: str = Field(..., min_length=8)

class UserSetNewPasswordSchema(BaseModel):
    old_password: str = Field(..., min_length=8)
    new_password: str = Field(..., min_length=8)