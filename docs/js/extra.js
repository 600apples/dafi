var elements = document.getElementsByClassName("md-header__inner")[0];
var str = '<a href="pdf/document.pdf" download><img src="./images/pdf-file.png" style="width: 30px; height: 30px; margin-left: 25px"></a>\n';
elements.insertAdjacentHTML( 'beforeend', str );

