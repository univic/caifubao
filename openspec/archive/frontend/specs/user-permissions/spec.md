> **⚠️ ARCHIVED — HISTORICAL REFERENCE ONLY**
> This file describes an abandoned architecture (Django, Celery, AkQuant).
> The current system uses Flask + datahub + Vue 3. Do NOT use as a current spec.
> See  for the active specification.

# User Permissions Specification

## ADDED Requirements

### Requirement: Role-based access control
The system SHALL support two user roles:
- **普通用户 (USER)**: Can view market data, run backtests, view signals
- **管理员 (ADM)**: All USER permissions + user management, system settings

#### Scenario: User role assignment
- **WHEN** user is created or modified by admin
- **THEN** user has a role assigned (USER or ADM)
- **AND** role determines accessible features

### Requirement: Permission-based UI rendering
The system SHALL show/hide UI elements based on user role.

#### Scenario: Regular user sees standard menu
- **WHEN USER logs** a in
- **THEN** sidebar shows: Dashboard, 历史行情, 回测, 信号与机会
- **AND** admin-only items are hidden

#### Scenario: Admin sees full menu
- **WHEN** an ADM logs in
- **THEN** sidebar additionally shows: 用户管理, 系统设置

### Requirement: User management (Admin only)
The system SHALL allow administrators to manage users.

#### Scenario: View user list
- **WHEN** admin navigates to /admin/users
- **THEN** a table shows all users with: username, email, role, status, created date

#### Scenario: Edit user
- **WHEN** admin clicks "编辑" on a user
- **THEN** a form allows changing: email, role, status

#### Scenario: Disable user
- **WHEN** admin clicks "禁用" on a user
- **THEN** user's status is set to locked
- **AND** user cannot log in

#### Scenario: Delete user
- **WHEN** admin clicks "删除" on a user
- **THEN** confirmation dialog appears
- **AND** on confirm, user is permanently deleted

### Requirement: User profile
The system SHALL allow users to view and edit their own profile.

#### Scenario: View profile
- **WHEN** user clicks "个人资料"
- **THEN** profile page shows: username, email, role, registration date, last login

#### Scenario: Edit own profile
- **WHEN** user clicks "编辑资料"
- **THEN** form allows changing email and password
- **AND** username cannot be changed

### Requirement: API-level permission enforcement
The system SHALL enforce permissions at both UI and API levels.

#### Scenario: API rejects unauthorized action
- **WHEN** USER tries to access admin API endpoint
- **THEN** API returns 403 Forbidden
- **AND** frontend shows error message

### Requirement: Permission denied page
The system SHALL show a friendly error when user lacks permission.

#### Scenario: Access denied
- **WHEN** user visits a page they don't have permission for
- **THEN** a "无权限访问" page is shown
- **AND** user can navigate back to allowed pages
