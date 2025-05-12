
from flask import session, redirect, url_for, Response
from functools import update_wrapper


def check_authorized_user(authorized_users):
    if authorized_users == "*":
        return True
    email = session["user_email"]
    authorized_users = authorized_users.split(',')
    authorized_users = [x.strip() for x in authorized_users]
    if email in authorized_users or '*@' + email.split('@')[1] in authorized_users:
        return True
    return False


class login_required:
    def __init__(self, authorized_users):
        self.authorized_users = authorized_users

    def __call__(self, func):
        def inner(*args, **kwargs):
            if not session.get("user_logged_in"):
                return redirect(url_for("gauth.login"))
            if check_authorized_user(self.authorized_users):
                return func(*args, **kwargs)
            else:
                return Response(status=403)
        return update_wrapper(inner, func)