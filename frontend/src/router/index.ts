import { createRouter, createWebHistory } from 'vue-router'
import type { RouteRecordRaw } from 'vue-router'
import MainLayout from '@/components/layout/MainLayout.vue'

const routes: RouteRecordRaw[] = [
  {
    path: '/welcome',
    name: 'Home',
    component: () => import('@/views/Home/HomeView.vue'),
    meta: { requiresAuth: false }
  },
  {
    path: '/login',
    name: 'Login',
    component: () => import('@/views/Auth/LoginView.vue'),
    meta: { requiresAuth: false }
  },
  {
    path: '/register',
    name: 'Register',
    component: () => import('@/views/Auth/RegisterView.vue'),
    meta: { requiresAuth: false }
  },
  {
    path: '/forgot-password',
    name: 'ForgotPassword',
    component: () => import('@/views/Auth/ForgotPasswordView.vue'),
    meta: { requiresAuth: false }
  },
  {
    path: '/reset-password',
    name: 'ResetPassword',
    component: () => import('@/views/Auth/ResetPasswordView.vue'),
    meta: { requiresAuth: false }
  },
  {
    path: '/',
    component: MainLayout,
    meta: { requiresAuth: true },
    children: [
      {
        path: '',
        name: 'Dashboard',
        component: () => import('@/views/Dashboard/DashboardView.vue')
      },
      {
        path: 'market',
        name: 'Market',
        component: () => import('@/views/Market/MarketView.vue')
      },
      {
        path: 'history',
        name: 'History',
        component: () => import('@/views/History/HistoryView.vue')
      },
      {
        path: 'history/:symbol',
        redirect: (to) => ({
          path: '/history',
          query: { symbol: Array.isArray(to.params.symbol) ? to.params.symbol[0] : String(to.params.symbol || '') }
        })
      },
      {
        path: 'backtest',
        name: 'BacktestList',
        component: () => import('@/views/Backtest/BacktestListView.vue')
      },
      {
        path: 'backtest/new',
        name: 'BacktestCreate',
        component: () => import('@/views/Backtest/BacktestCreateView.vue')
      },
      {
        path: 'backtest/:id',
        name: 'BacktestResult',
        component: () => import('@/views/Backtest/BacktestResultView.vue')
      },
      {
        path: 'signals',
        name: 'Signals',
        component: () => import('@/views/Signals/SignalsView.vue')
      },
      {
        path: 'score-experiments',
        name: 'ScoreExperiments',
        component: () => import('@/views/ScoreExperiments/ScoreExperimentsView.vue')
      },
      {
        path: 'indices',
        name: 'Indices',
        component: () => import('@/views/Indices/IndicesView.vue')
      },
      {
        path: 'data-quality',
        name: 'DataQuality',
        component: () => import('@/views/DataQuality/DataQualityView.vue')
      },
      {
        path: 'admin/users',
        name: 'UserManagement',
        component: () => import('@/views/Admin/UserManagementView.vue'),
        meta: { requiresAdmin: true }
      },
      {
        path: 'profile',
        name: 'Profile',
        component: () => import('@/views/Auth/ProfileView.vue')
      }
    ]
  },
  {
    path: '/app',
    redirect: '/'
  },
  {
    path: '/:pathMatch(.*)*',
    name: 'NotFound',
    component: () => import('@/views/Error/NotFoundView.vue')
  },
  {
    path: '/403',
    name: 'Forbidden',
    component: () => import('@/views/Error/ForbiddenView.vue')
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router
