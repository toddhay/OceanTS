module.exports = {
    // 
    // "extends": "eslint:recommended",
    "extends": ["plugin:@typescript-eslint/recommended"],
    "parser": "@typescript-eslint/parser",
    "plugins": ["@typescript-eslint"],
    "eslint.validate": [
        "javascript",
        "typescript",
      ],
    "rules": {

        "indent": ["error", 4],
        "linebreak-style": ["error", "unix"],
        "quotes": ["error", "double"],
        "semi": ["error", "always"],

        "comma-dangle": ["error", "always"],
        "no-cond-assign": ["error", "always"],

        "no-console": "off"
    }
}