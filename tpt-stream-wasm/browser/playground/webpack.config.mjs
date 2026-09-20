import path from 'node:path';
import { fileURLToPath } from 'node:url';
import CopyPlugin from 'copy-webpack-plugin';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Builds the in-browser pipeline playground into playground/dist/.
export default {
  mode: 'production',
  devtool: false,
  target: 'web',
  entry: path.resolve(__dirname, 'main.js'),
  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: 'playground.js',
    clean: true,
  },
  experiments: {
    asyncWebAssembly: true,
  },
  optimization: {
    minimize: false,
  },
  plugins: [
    new CopyPlugin({
      patterns: [{ from: path.resolve(__dirname, 'index.html'), to: 'index.html' }],
    }),
  ],
};
