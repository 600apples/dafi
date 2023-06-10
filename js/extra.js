// Google Analytics
function loadJS(file_url, async = true) {
  let scriptEle = document.createElement("script");

  scriptEle.setAttribute("src", file_url);
  scriptEle.setAttribute("type", "text/javascript");
  scriptEle.setAttribute("async", async);

  document.head.appendChild(scriptEle);

  // success event
  scriptEle.addEventListener("load", () => {
    console.log("File loaded")
  });
   // error event
  scriptEle.addEventListener("error", (ev) => {
    console.log("Error on loading file", ev);
  });
}

loadJS("https://www.googletagmanager.com/gtag/js?id=G-2N4D4W3NVK");
window.dataLayer = window.dataLayer || [];
window.gtag = function() {
  window.dataLayer.push(arguments);
};

window.gtag('js', new Date());
window.gtag('config', 'G-2N4D4W3NVK');

// Links
const locationNesting = location.pathname.replace(/^\/+|\/+$/g, '').split("/").length;
let relativeNesting;

switch (locationNesting) {
    case 1:
        relativeNesting = "."; break;
    case 2:
        relativeNesting = ".."; break;
    case 3:
        relativeNesting = "../.."; break;
}

let elements = document.getElementsByClassName("md-header__inner")[0];
let pdfFileLink = `<a href="${relativeNesting}/pdf/daffi.pdf" download><img src="${relativeNesting}/images/pdf-file.png" style="width: 30px; height: 30px; margin-left: 25px"></a>\n`;
elements.insertAdjacentHTML( 'beforeend', pdfFileLink );

let gitterLink = `<a href="https://app.gitter.im/#/room/%23daffi:gitter.im"><img src="${relativeNesting}/images/gitter.png" style="width: 30px; height: 37px; margin-left: 33px"></a>\n`;
elements.insertAdjacentHTML( 'beforeend', gitterLink );
