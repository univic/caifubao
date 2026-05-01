# -*- coding: utf-8 -*-
# Admin API Blueprint - User Management

from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt
from flask_security import hash_password
from app.model.user_model import User

admin_bp = Blueprint("admin", __name__, url_prefix="/api/admin")


def check_admin():
    """Check if current user is admin"""
    claims = get_jwt()
    roles = claims.get("role", [])
    return "ADM" in roles


@admin_bp.route("/users", methods=["GET"])
@jwt_required()
def get_user_list():
    """Get user list (admin only)"""
    if not check_admin():
        return jsonify({"message": "Admin access required"}), 403

    page = request.args.get("page", 1, type=int)
    page_size = request.args.get("page_size", 20, type=int)
    keyword = request.args.get("keyword", "")

    query = {}
    if keyword:
        query["$or"] = [
            {"username": {"$regex": keyword}},
            {"email": {"$regex": keyword}},
        ]

    total = User.objects(__raw__=query).count()
    users = User.objects(__raw__=query).skip((page - 1) * page_size).limit(page_size)

    return jsonify(
        {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": [
                {
                    "id": str(u.id),
                    "username": u.username,
                    "email": u.email,
                    "role": u.user_role,
                    "user_status": u.user_status,
                    "time_registered": u.time_registered.isoformat()
                    if u.time_registered
                    else None,
                }
                for u in users
            ],
        }
    ), 200


@admin_bp.route("/users/<user_id>", methods=["GET"])
@jwt_required()
def get_user(user_id):
    """Get user detail (admin only)"""
    if not check_admin():
        return jsonify({"message": "Admin access required"}), 403

    user = User.objects(id=user_id).first()
    if not user:
        return jsonify({"message": "User not found"}), 404

    return jsonify(
        {
            "id": str(user.id),
            "username": user.username,
            "email": user.email,
            "role": user.user_role,
            "user_status": user.user_status,
            "time_registered": user.time_registered.isoformat()
            if user.time_registered
            else None,
            "last_login_at": user.last_login_at.isoformat()
            if user.last_login_at
            else None,
        }
    ), 200


@admin_bp.route("/users", methods=["POST"])
@jwt_required()
def create_user():
    """Create user (admin only)"""
    if not check_admin():
        return jsonify({"message": "Admin access required"}), 403

    data = request.get_json()
    username = data.get("username")
    email = data.get("email")
    password = data.get("password")
    role = data.get("role", ["USER"])

    if not all([username, email, password]):
        return jsonify({"message": "Missing required fields"}), 400

    if User.objects(username=username).first():
        return jsonify({"message": "Username already exists"}), 400

    if User.objects(email=email).first():
        return jsonify({"message": "Email already exists"}), 400

    user = User(
        username=username,
        email=email,
        password_hash=hash_password(password),
        user_status=["20"],
        user_role=role,
    )
    user.save()

    return jsonify(
        {
            "id": str(user.id),
            "username": user.username,
            "email": user.email,
            "role": user.user_role,
        }
    ), 201


@admin_bp.route("/users/<user_id>", methods=["PUT"])
@jwt_required()
def update_user(user_id):
    """Update user (admin only)"""
    if not check_admin():
        return jsonify({"message": "Admin access required"}), 403

    user = User.objects(id=user_id).first()
    if not user:
        return jsonify({"message": "User not found"}), 404

    data = request.get_json()

    if "email" in data:
        existing = User.objects(email=data["email"]).first()
        if existing and str(existing.id) != user_id:
            return jsonify({"message": "Email already in use"}), 400
        user.email = data["email"]

    if "role" in data:
        user.user_role = data["role"]

    if "user_status" in data:
        user.user_status = data["user_status"]

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


@admin_bp.route("/users/<user_id>", methods=["DELETE"])
@jwt_required()
def delete_user(user_id):
    """Delete user (admin only)"""
    if not check_admin():
        return jsonify({"message": "Admin access required"}), 403

    user = User.objects(id=user_id).first()
    if not user:
        return jsonify({"message": "User not found"}), 404

    user.delete()

    return jsonify({"message": "User deleted successfully"}), 200


@admin_bp.route("/users/<user_id>/disable", methods=["POST"])
@jwt_required()
def disable_user(user_id):
    """Disable user (admin only)"""
    if not check_admin():
        return jsonify({"message": "Admin access required"}), 403

    user = User.objects(id=user_id).first()
    if not user:
        return jsonify({"message": "User not found"}), 404

    if "99" not in user.user_status:
        user.user_status.append("99")
        user.save()

    return jsonify({"message": "User disabled"}), 200


@admin_bp.route("/users/<user_id>/enable", methods=["POST"])
@jwt_required()
def enable_user(user_id):
    """Enable user (admin only)"""
    if not check_admin():
        return jsonify({"message": "Admin access required"}), 403

    user = User.objects(id=user_id).first()
    if not user:
        return jsonify({"message": "User not found"}), 404

    user.user_status = [s for s in user.user_status if s != "99"]
    if "20" not in user.user_status:
        user.user_status.append("20")
    user.save()

    return jsonify({"message": "User enabled"}), 200
