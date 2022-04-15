import vue from '@vitejs/plugin-vue'
import path from 'path'

export default {
  open: true,
  alias: {
    '/@': path.resolve(__dirname, './src')
  },
  plugins: [vue()]
}
