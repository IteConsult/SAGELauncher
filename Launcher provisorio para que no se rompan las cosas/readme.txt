*En la carpeta donde tengas el L2.py tenés que tener los siguientes archivos:
	*Las tablas de los servicios REST (BOM, ItemMaster, Inventory, RoutingAndRates, WorkOrders, Workcenters, Facility).
	*La tabla manual Model_Workcenters (es una tabla estática)
	*La tabla MD_Bulk_Code.csv (pedísela a Juan de última, se actualiza lunes/jueves)
	*Extruders_Schedule.xlsx (pedísela a Juan, cambia cada tanto)
*Las tablas de los servicios REST las podés bajar poniendo en la carpeta los archivos Retrieve.py y Retrieve.bat y ejecutando el Retrieve.bat.
Eso ejecuta un script que te baja las tablas REST como Excel (y las sube a HANA, para Juan).
*IMPORTANTE: pedirle a juan que me explique lo de la VPN (como conectarme)

*El botón que vas a usar es el Generate Excels. Este botón lee los Excels (REST + los dos archivos manuales) y te genera los archivos para el modelo. Hay un checkbox que sube los archivos del modelo a HANA
por si en algún momento quieren hacer la prueba de leer desde HANA en AnyLogic.
*Las tablas de los REST estaría bueno revisarlas cada tanto para ver si están mandando algún dato nuevo.