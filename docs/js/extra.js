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
let str = `<a href="${relativeNesting}/pdf/daffi.pdf" download><img src="${relativeNesting}/images/pdf-file.png" style="width: 30px; height: 30px; margin-left: 25px"></a>\n`;
elements.insertAdjacentHTML( 'beforeend', str );
