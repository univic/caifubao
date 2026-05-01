/// <reference types="vite/client" />

declare module '*.vue' {
  import type { DefineComponent } from 'vue'
  const component: DefineComponent<object, object, unknown>
  export default component
}

declare module 'element-plus/dist/locale/zh-cn.mjs' {
  import type { Locale } from 'element-plus'
  const zhCn: Locale
  export default zhCn
}
