/// <reference types="vitest" />
import { defineConfig } from "vite";
// import vue from "@vitejs/plugin-vue";
// https://vitejs.dev/config/

export default defineConfig({
    test: {
        // add your test file to the "files" array
        files: ['./tests/*.test.js']
    },
});
