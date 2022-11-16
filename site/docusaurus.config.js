// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require("prism-react-renderer/themes/github");
const darkCodeTheme = require("prism-react-renderer/themes/dracula");

/** @type {import("@docusaurus/types").Config} */
const config = {
  title: "Pathling",
  tagline: "Analytics on FHIR&reg;",
  url: "https://pathling.csiro.au",
  baseUrl: "/",
  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",
  favicon: "favicon.ico",

  organizationName: "aehrc",
  projectName: "pathling",
  trailingSlash: false,

  i18n: {
    defaultLocale: "en",
    locales: ["en"]
  },

  presets: [
    [
      "classic",
      /** @type {import("@docusaurus/preset-classic").Options} */
      ({
        docs: {
          sidebarPath: require.resolve("./sidebars.js"),
          editUrl: "https://github.com/aehrc/pathling/tree/main/site/"
        },
        theme: {
          customCss: require.resolve("./src/css/custom.css")
        },
        sitemap: {}
      })
    ]
  ],

  themeConfig:
  /** @type {import("@docusaurus/preset-classic").ThemeConfig} */
    ({
      navbar: {
        title: null,
        logo: {
          alt: "Pathling",
          src: "assets/images/logo-colour.svg",
          srcDark: "assets/images/logo-colour-dark.svg",
          href: "https://pathling.csiro.au"
        },
        items: [
          {
            type: "doc",
            position: "left",
            docId: "index",
            label: "Overview"
          },
          {
            type: "docSidebar",
            position: "left",
            sidebarId: "libraries",
            label: "Libraries"
          },
          {
            type: "docSidebar",
            position: "left",
            sidebarId: "server",
            label: "Server"
          },
          {
            type: "docSidebar",
            position: "left",
            sidebarId: "fhirpath",
            label: "FHIRPath"
          },
          {
            type: "doc",
            docId: "roadmap",
            label: "Roadmap"
          },
          {
            href: "https://github.com/aehrc/pathling",
            label: "GitHub",
            position: "right"
          }
        ]
      },
      footer: {
        copyright: `This documentation is dedicated to the public domain via <a href="https://creativecommons.org/publicdomain/zero/1.0/">CC0</a>.`
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
        additionalLanguages: ["java", "scala", "yaml", "docker"]
      }
    })
};

module.exports = config;
