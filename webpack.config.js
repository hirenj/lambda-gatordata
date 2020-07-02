
const fs = require('fs');
const path = require('path');
const ZipPlugin = require('zip-webpack-plugin');

const GitRevisionPlugin = require('git-revision-webpack-plugin')
const gitRevisionPlugin = new GitRevisionPlugin({
      versionCommand: 'describe --always --tags --dirty'
    });

const RemovePlugin = require('remove-files-webpack-plugin');

function getEntries() {
  return fs.readdirSync('./lambdas/')
      .filter(
          (file) => file.match(/.*\.js$/)
      )
      .map((file) => {
          return {
              name: file.substring(0, file.length - 3),
              path: './lambdas/' + file
          }
      }).reduce((memo, file) => {
          memo[file.name] = file.path
          return memo;
      }, {})
}

const configBase = {
  target: 'node',
  entry: getEntries(),
  mode: 'production',
  output: {
    libraryTarget: "commonjs",
    filename: 'js/[name]/index.js'
  },
  optimization: {
    minimize: false,
  },
  externals:[{ "aws-sdk": "commonjs aws-sdk" }],
  devtool: 'inline-cheap-module-source-map',
};

const configPlugins = {
    plugins: Object.keys(configBase.entry).map((entryName) => {
        return new ZipPlugin({
            path: path.resolve(__dirname, 'dist/'),
            filename: `${entryName}-${gitRevisionPlugin.version()}`,
            include: [new RegExp(`${entryName}/.*`)],
            pathMapper: assetPath => path.basename(assetPath),
            extension: 'zip'
        })
    })
};
const config = Object.assign(configBase, configPlugins);

config.plugins.push(        new RemovePlugin({
            after: {
                include: [`./dist/js`],
                log: true,
            }
        })
);

module.exports = config