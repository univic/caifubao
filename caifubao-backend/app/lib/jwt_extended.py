from flask_jwt_extended import JWTManager
from app.model.user_model import User

jwt = JWTManager()


@jwt.user_identity_loader
def user_identity_loader(user):
    user_identity = {
        "id": str(user.id),
        "username": user.username,
        "user_status": user.user_status,
        "user_role": user.user_role
    }
    return user_identity


@jwt.user_lookup_loader
def user_identity_lookup(_jwt_header, jwt_data):
    identity = jwt_data["sub"]
    return User.objects(id=identity["id"]).first()
