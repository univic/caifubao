import { createApp } from 'vue'
import store from './store/index'
import App from './App.vue'
import ElementPlus from 'element-plus'
import 'element-plus/lib/theme-chalk/index.css'
import './index.css'
import Router from "./router"  

const app = createApp(App)


app.use(store)
app.use(ElementPlus)
app.use(Router)
app.mount('#app')