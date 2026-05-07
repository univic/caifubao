# -*- coding: utf-8 -*-
# Auth API Blueprint

from flask import Blueprint, request, jsonify
from flask_jwt_extended import (
    create_access_token,
    create_refresh_token,
    jwt_required,
    get_jwt_identity,
)
from flask_security import (
    MongoEngineUserDatastore,
    Security,
    hash_password,
    verify_password,
)
from app.lib.db_watcher.mongoengine_tool import db
from app.model.user_model import User
from app.model.user_role import UserRole
import datetime

auth_bp = Blueprint("auth", __name__, url_prefix="/api/auth")

# Initialize Flask-Security
user_datastore = MongoEngineUserDatastore(db, User, UserRole)
security = Security()


def init_auth(app):
    security.init_app(app, user_datastore)


MAX_FAILED_LOGIN_ATTEMPTS = 5
LOCKOUT_DURATION_MINUTES = 30


@auth_bp.route("/login", methods=["POST"])
def login():
    """User login"""
    data = request.get_json()

    username = data.get("username")
    password = data.get("password")

    if not all([username, password]):
        return jsonify({"message": "Missing username or password"}), 400

    # Find user
    user = User.objects(username=username).first()

    # Check if account is locked
    if user and user.locked_until and datetime.datetime.now() < user.locked_until:
        remaining_time = (user.locked_until - datetime.datetime.now()).seconds // 60
        return jsonify(
            {
                "message": f"Account is locked. Please try again in {remaining_time} minutes"
            }
        ), 403

    if not user or not verify_password(password, user.password_hash):
        # Increment failed login count
        if user:
            user.failed_login_count = (user.failed_login_count or 0) + 1

            # Lock account if max attempts reached
            if user.failed_login_count >= MAX_FAILED_LOGIN_ATTEMPTS:
                user.locked_until = datetime.datetime.now() + datetime.timedelta(
                    minutes=LOCKOUT_DURATION_MINUTES
                )
                if "99" not in user.user_status:
                    user.user_status.append("99")

            user.save()

        return jsonify({"message": "Invalid username or password"}), 401

    # Check user status
    if "99" in user.user_status and (
        not user.locked_until or datetime.datetime.now() >= user.locked_until
    ):
        # Remove lock if lockout period has expired
        user.user_status.remove("99")
    elif "99" in user.user_status:
        return jsonify({"message": "Account is locked"}), 403

    # Update user state and save once before token creation
    user.failed_login_count = 0
    user.locked_until = None
    user.last_login_at = user.current_login_at
    user.last_login_ip = user.current_login_ip
    user.current_login_at = datetime.datetime.now()
    user.current_login_ip = request.remote_addr
    user.login_count = (user.login_count or 0) + 1
    user.save()

    # Create tokens
    access_token = create_access_token(
        identity=str(user.id),
        additional_claims={"username": user.username, "role": user.user_role},
        expires_delta=datetime.timedelta(minutes=30),
    )
    refresh_token = create_refresh_token(
        identity=str(user.id),
        expires_delta=datetime.timedelta(days=7),
    )

    return jsonify(
        {
            "token": access_token,
            "refresh_token": refresh_token,
            "user": {
                "id": str(user.id),
                "username": user.username,
                "email": user.email,
                "role": user.user_role,
                "user_status": user.user_status,
            },
        }
    ), 200


@auth_bp.route("/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh():
    """Refresh access token"""
    identity = get_jwt_identity()
    user = User.objects(id=identity).first()

    if not user:
        return jsonify({"message": "User not found"}), 404

    access_token = create_access_token(
        identity=identity,
        additional_claims={"username": user.username, "role": user.user_role},
    )

    return jsonify({"token": access_token}), 200


@auth_bp.route("/user", methods=["GET"])
@jwt_required()
def get_user():
    """Get current user info"""
    identity = get_jwt_identity()
    user = User.objects(id=identity).first()

    if not user:
        return jsonify({"message": "User not found"}), 404

    return jsonify(
        {
            "id": str(user.id),
            "username": user.username,
            "email": user.email,
            "role": user.user_role,
            "user_status": user.user_status,
        }
    ), 200


@auth_bp.route("/logout", methods=["POST"])
@jwt_required()
def logout():
    """User logout (client-side token removal)"""
    # In a stateless JWT setup, logout is handled client-side
    # For token blacklisting, you'd need additional implementation
    return jsonify({"message": "Logged out successfully"}), 200


@auth_bp.route("/change-password", methods=["POST"])
@jwt_required()
def change_password():
    """Change password"""
    data = request.get_json()
    identity = get_jwt_identity()
    user = User.objects(id=identity).first()

    if not user:
        return jsonify({"message": "User not found"}), 404

    old_password = data.get("old_password")
    new_password = data.get("new_password")

    if not verify_password(old_password, user.password_hash):
        return jsonify({"message": "Incorrect old password"}), 400

    user.password_hash = hash_password(new_password)
    user.save()

    return jsonify({"message": "Password changed successfully"}), 200


@auth_bp.route("/forgot-password", methods=["POST"])
def forgot_password():
    """Request password reset"""
    data = request.get_json(silent=True) or {}
    email = data.get("email")

    if not email:
        return jsonify({"message": "Email is required"}), 400

    user = User.objects(email=email).first()
    if user:
        # TODO: Generate a short-lived reset token and send it through email.
        # Never return reset credentials in the API response.
        pass

    # Do not reveal whether the email exists.
    return jsonify({"message": "If email exists, reset link will be sent"}), 200


@auth_bp.route("/reset-password", methods=["POST"])
def reset_password():
    """Reset password with token"""
    data = request.get_json()
    token = data.get("token")
    new_password = data.get("password")

    if not all([token, new_password]):
        return jsonify({"message": "Missing token or password"}), 400

    # Verify token and get identity
    from flask_jwt_extended import decode_token

    try:
        decoded = decode_token(token)
        user_id = decoded["sub"]
        if decoded.get("type") != "password_reset":
            return jsonify({"message": "Invalid token type"}), 400
    except Exception:
        return jsonify({"message": "Invalid or expired token"}), 400

    user = User.objects(id=user_id).first()
    if not user:
        return jsonify({"message": "User not found"}), 404

    user.password_hash = hash_password(new_password)
    user.save()

    return jsonify({"message": "Password reset successfully"}), 200


@auth_bp.route("/profile", methods=["PUT"])
@jwt_required()
def update_profile():
    """Update user profile"""
    identity = get_jwt_identity()
    user = User.objects(id=identity).first()

    if not user:
        return jsonify({"message": "User not found"}), 404

    data = request.get_json()

    if "email" in data:
        # Check if email is taken
        existing = User.objects(email=data["email"]).first()
        if existing and str(existing.id) != str(user.id):
            return jsonify({"message": "Email already in use"}), 400
        user.email = data["email"]

    user.save()

    return jsonify(
        {
            "id": str(user.id),
            "username": user.username,
            "email": user.email,
            "role": user.user_role,
            "user_status": user.user_status,
        }
    ), 200
