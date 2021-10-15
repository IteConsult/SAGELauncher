Si no hay .spec:
pyinstaller AlphiaLauncher.py --onefile --windowed
(--onefile: para que se nos haga en un solo archivo
--windowed: para que no se abra una consola cuando ejecutamos el .exe)

Si ya hay .spec:
pyinstaller AlphiaLauncher.spec

OJO:
A veces nos confundimos y ejecutamos "pyinstaller AlphiaLauncher.py". Esto lo que hace es generarnos el Launcher pero en muchos archivos y además cada vez que lo abrimos se nos abre una consola. ENCIMA nos sobreescribe el .spec con las nuevas especificaciones que están mal (o sea, en muchos archivos y con consola). Por eso, ejecutar "pyinstaller AlphiaLauncher.spec" ahora ya no funciona tampoco. Sí o sí hay que volver a ejecutar la línea completa "pyinstaller AlphiaLauncher.py --onefile --windowed". Esto nos genera el Launcher con las especificaciones que queremos y además nos arregla el .spec para la próxima.