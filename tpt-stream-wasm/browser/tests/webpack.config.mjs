import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default {
  mode: 'production',
  devtool: false,
  target: 'node',
  entry: path.resolve(__dirname, 'harness.js'),
  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: 'bundle.cjs',
    clean: true,
  },
  experiments: {
    asyncWebAssembly: true,
  },
  optimization: {
    minimize: false,
  },
};