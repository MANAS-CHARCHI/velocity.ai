from django.urls import path
from . import views
urlpatterns = [
    path('register/', views.UserRegisterView.as_view(), name='user-register'),
    path('activate/<str:email>/<str:token>/', views.UserActivateView.as_view(), name='user-activate'),
    path('login/', views.UserLoginView.as_view(), name='user-login'),
    path('profile/', views.UserProfileView.as_view(), name='user-profile'),
    path('logout/', views.UserLogoutView.as_view(), name='user-logout'),
    path('passwordreset/', views.UserResetPasswordView.as_view(), name='user-reset-password'),
    path('passwordreset/confirm/<str:email>/<str:token>/', views.UserPasswordResetConfirmView.as_view(), name='user-password-reset-confirm'),
    path('passwordchange/', views.UserSetNewPasswordView.as_view(), name='user-set-new-password'),
]