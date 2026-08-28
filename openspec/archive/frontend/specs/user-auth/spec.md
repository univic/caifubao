> **⚠️ ARCHIVED — HISTORICAL REFERENCE ONLY**
> This file describes an abandoned architecture (Django, Celery, AkQuant).
> The current system uses Flask + datahub + Vue 3. Do NOT use as a current spec.
> See `openspec/archive/mvp-quant-demo/` for the archived specification.

# User Authentication Specification

## ADDED Requirements

### Requirement: User login
The system SHALL allow users to log in with username/email and password.

#### Scenario: Successful login
- **WHEN** user enters valid credentials and clicks "登录"
- **THEN** user is redirected to Dashboard
- **AND** JWT token is stored in localStorage
- **AND** user info is stored in Pinia store

#### Scenario: Invalid credentials
- **WHEN** user enters wrong password
- **THEN** an error message "用户名或密码错误" is displayed
- **AND** password field is cleared

#### Scenario: Session timeout
- **WHEN** user refreshes page after token expired
- **THEN** user is redirected to login page
- **AND** a message "登录已过期，请重新登录" is shown

### Requirement: User registration
The system SHALL allow new users to register with username, email, and password.

#### Scenario: Register new account
- **WHEN** user fills registration form with valid data
- **THEN** account is created and user is redirected to login
- **AND** a success message "注册成功，请登录" is shown

#### Scenario: Duplicate username/email
- **WHEN** user tries to register with existing username or email
- **THEN** error message "用户名/邮箱已被注册" is displayed

#### Scenario: Password strength validation
- **WHEN** user enters weak password
- **THEN** a hint shows minimum requirements (至少8位，包含数字和字母)

### Requirement: User logout
The system SHALL allow users to log out securely.

#### Scenario: User logout
- **WHEN** user clicks "退出登录"
- **THEN** JWT token is removed from localStorage
- **AND** user is redirected to login page

### Requirement: Password reset
The system SHALL allow users to reset their password via email.

#### Scenario: Request password reset
- **WHEN** user clicks "忘记密码" and enters email
- **THEN** a reset link is sent to user's email
- **AND** a message "重置链接已发送到邮箱" is shown

#### Scenario: Set new password
- **WHEN** user clicks reset link and enters new password
- **THEN** password is updated
- **AND** user is redirected to login

### Requirement: Current user info display
The system SHALL display current user's info in the header.

#### Scenario: Show user info
- **WHEN** user is logged in
- **THEN** header shows username and avatar
- **AND** clicking avatar shows dropdown menu (profile, settings, logout)

### Requirement: Protected routes
The system SHALL restrict access to authenticated pages.

#### Scenario: Access protected page without login
- **WHEN** user tries to access `/backtest` without login
- **THEN** user is redirected to login page
- **AND** after login, redirected back to original page

### Requirement: JWT token refresh
The system SHALL automatically refresh JWT token before expiration.

#### Scenario: Token refresh
- **WHEN** token is about to expire (within 5 minutes)
- **THEN** system requests new token silently
- **AND** user continues without interruption
