import email
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework_simplejwt.tokens import RefreshToken
from . import models, utils, serializers, schemas

class UserRegisterView(APIView):
    permission_classes = [AllowAny]
    def post(self, request, *args, **kwargs):
        payload, errors= utils.validate(schemas.UserRegisterSchema, request.data)
        if errors:
            return Response({'errors': errors}, status=status.HTTP_400_BAD_REQUEST)
        if models.User.objects.filter(email=payload.email).exists():
            return Response({'error': 'Email already exists'}, status=status.HTTP_400_BAD_REQUEST)
        try:
            models.User.objects.create_user(email=payload.email, password=payload.password, user_name=payload.email)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        return Response({'message': 'User registered successfully, Activation link shared view email.'}, status=status.HTTP_201_CREATED)

class UserLoginView(APIView):
    permission_classes = [AllowAny]
    def post(self, request):
        payload, errors = utils.validate(schemas.UserLoginSchema, request.data)
        if errors:
            return Response({'errors': errors}, status=status.HTTP_400_BAD_REQUEST)
        try:
            user = models.User.objects.filter(email=payload.email).first()
            if not user.is_active:
                return Response({'error': 'User account is not active, Check you email to active the account'}, status=status.HTTP_403_FORBIDDEN)
            if not user or not user.check_password(payload.password):
                return Response({'error': 'Invalid email or password'}, status=status.HTTP_401_UNAUTHORIZED)
            refresh= RefreshToken.for_user(user)
        except models.User.DoesNotExist:
            user = None
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        return Response({'refresh': str(refresh), 'access': str(refresh.access_token), 'user': serializers.UserSerializer(user).data}, status=status.HTTP_200_OK)
    
class UserProfileView(APIView):
    permission_classes = [IsAuthenticated]
    def get(self, request):
        return Response(serializers.UserSerializer(request.user).data, status=status.HTTP_200_OK)
          
class UserLogoutView(APIView):
    permission_classes = [IsAuthenticated]
    def post(self, request):
        payload, errors = utils.validate(schemas.UserLogoutSchema, request.data)
        if errors:
            return Response({'errors': errors}, status=status.HTTP_400_BAD_REQUEST)
        refresh_token = payload.refresh
        try:
            token = RefreshToken(refresh_token)
            token.blacklist()
            return Response({'message': 'User logged out successfully'}, status=status.HTTP_205_RESET_CONTENT)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

class UserActivateView(APIView):
    permission_classes = [AllowAny]
    def post(self, request, *args, **kwargs):
        payload, errors = utils.validate(schemas.UserActivateSchema, kwargs)
        if errors:
            return Response({'errors': errors}, status=status.HTTP_400_BAD_REQUEST)
        try:
            user_token = models.UserActiveToken.objects.get(token=payload.token, user__email=payload.email)
            if user_token:
                user = user_token.user
                user.is_active = True
                user.save()
                user_token.delete()
                return Response({'message': 'User account activated successfully'}, status=status.HTTP_200_OK)
            else:
                return Response({'error': 'Invalid activation token'}, status=status.HTTP_400_BAD_REQUEST)
        except models.UserActiveToken.DoesNotExist:
            return Response({'error': 'Invalid activation token'}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class UserResetPasswordView(APIView):
    permission_classes = [AllowAny]
    def post(self, request):
        payload, errors = utils.validate(schemas.UserResetPasswordSchema, request.data)
        if errors:
            return Response({'errors': errors}, status=status.HTTP_400_BAD_REQUEST)
        try:
            user = models.User.objects.filter(email=payload.email).first()
            if user:
                token = utils.create_token()
                models.UserPasswordResetToken.objects.create(user=user, token=token)
                # TODO: Send password reset email with the token
                return Response({'message': 'Password reset link sent to email'}, status=status.HTTP_200_OK)
            else:
                return Response({'error': 'User with this email does not exist'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class UserPasswordResetConfirmView(APIView):
    permission_classes = [AllowAny]
    def post(self, request, *args, **kwargs):
        data={**kwargs, **request.data}
        payload, errors = utils.validate(schemas.UserPasswordResetSchema, data)
        if errors:
            return Response({'errors': errors}, status=status.HTTP_400_BAD_REQUEST)
        try:
            user_token=models.UserPasswordResetToken.objects.get(token=payload.token, user__email=payload.email)
            if user_token:
                user = user_token.user
                user.set_password(payload.new_password)
                try:
                    user.save()
                    user_token.delete()
                except Exception as e:
                    return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
                return Response({'message': 'Password reset successfully'}, status=status.HTTP_200_OK)
            else:
                return Response({'error': 'Invalid password reset token'}, status=status.HTTP_400_BAD_REQUEST)
        except models.UserPasswordResetToken.DoesNotExist:
            return Response({'error': 'Invalid password reset token'}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class UserSetNewPasswordView(APIView):
    permission_classes = [IsAuthenticated]
    def post(self, request):
        payload, errors = utils.validate(schemas.UserSetNewPasswordSchema, request.data)
        if errors:
            return Response({'errors': errors}, status=status.HTTP_400_BAD_REQUEST)
        user = request.user
        if not user.check_password(payload.old_password):
            return Response({'error': 'Old password is incorrect'}, status=status.HTTP_400_BAD_REQUEST)
        user.set_password(payload.new_password)
        user.save()
        return Response({'message': 'Password updated successfully'}, status=status.HTTP_200_OK)