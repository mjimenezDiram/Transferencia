"""
Created on Tue Aug 11 12:25:36 2020

@author: lvazquez

Procedimiento de extraccion de PQZs para todos los G44X0's'

"""
import os.path
from ftplib import FTP
import datetime


class G44X0PQZs:

    def RecoverProcess(ips, logFile, debug, folderDict, clientesDict,
                       plantasDict):

        def pickDay(day, dfolders):
            for d in dfolders:
                if d.find(day) == 0:
                    ftp.cwd(d)
                    if not os.path.exists(d):
                        os.mkdir(d)
                    os.chdir(d)
                    break
            return d

        def logSave(logFile, m, debug):
            NOW = datetime.datetime.now()  # Tiempo exacto actual
            NOW_log = str(NOW.strftime('[%Y'+'-'+'%m'+'-'+'%d]' +
                                       '[%H:'+'%M:'+'%S]'))
            log = NOW_log+'~'+m
            if debug == '1':
                logFile.write(log+'\n')
            print(log)
        # ¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬
        print('Medidor G44X0')
        # ¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬
        port = 21
        # Registramos el folder de trabajo
        temp_a = os.getcwd()
        # Registramos la fecha a descargar con los argumentos
        # Registramos la fecha a descargar
        today = datetime.date.today()
        oneday = datetime.timedelta(days=1)
        yesterday = today-oneday
        yesterday = yesterday-oneday
        year = str(yesterday.year)
        month = str(yesterday.month)
        day = str(yesterday.day)
        host = ips  # Multiples IPs
        logSave(logFile, 'Parametros de fecha definidos', debug)
        # ¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬
        for HOST in host:  # Para cada IP diferente
            logSave(logFile, "Estableciendo conexion FTP para el host " +
                    HOST+"...", debug)
            # Conexion FTP
            ftp = FTP()
            try:
                connFtp = ftp.connect(HOST, port)
                connFtp
                ftp.login('ftpuser', 'ftppassword')  # Linea para G4420
            except Exception:
                logSave(logFile, 'Conexion con el host ' +
                        HOST+' no establecida1', debug)
                continue
            logSave(logFile, "Conexion FTP establecida para el host " +
                    HOST, debug)
    # ¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬
            logSave(logFile,
                    "Iniciando definicion de folder iniciales...",
                    debug)
            # Definimos folder iniciales
            # pathlocal = ('Z:\\CLOUD-PQ\\MedicionRemota\\')
            pathlocal = ('C:\\Users\\mjimenez\\Documents\\MedicionRemota')
            # Es necesario crear una carpeta para cada medidor
            os.chdir(pathlocal)  # Moverse a pathlocal en OS
            ftp.cwd('/CF_UPMB/PQZIPDATA_')  # Linea para el medidor G44X0
            # Localmente debo revisar si existe una carpeta del medidor
            # Sino, crearla
            if not os.path.exists(pathlocal+'\\'+clientesDict[HOST]+'\\' +
                                  plantasDict[HOST]):
                os.makedirs(pathlocal+'\\'+clientesDict[HOST]+'\\' +
                            plantasDict[HOST])

            pathlocal = (pathlocal+'\\'+clientesDict[HOST]+'\\' +
                         plantasDict[HOST] + '\\' + folderDict[HOST])
            # print('Folder actual:' + str(os.getcwd()))
            os.chdir(pathlocal)
            # print('Folder actual:' + str(os.getcwd()))

            if not os.path.exists(pathlocal+'\\'+'PQZ\\'):
                os.mkdir(pathlocal+'\\'+'PQZ\\')

            pathlocal = pathlocal+'\\'+'PQZ\\'
            os.chdir(pathlocal)
            print('Folder actual:' + str(os.getcwd()))
            # Path de la ip que se esta manejando
            pathip = pathlocal  # + '\\' + folderDict[HOST] + '\\PQZ\\'
            os.chdir(pathip)  # Moverse a la carpeta
    # ¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬
            # Año
            yfolders = []
            yfolders = ftp.nlst()
            foundYear = []
            for y in range(0, len(yfolders)):
                if yfolders[y].find(year) == 0:  # Si un año coincide
                    foundYear.append(yfolders[y])  # Guardalo

            if foundYear == []:
                logSave(logFile,
                        'No se encontro una carpeta para el año buscado (' +
                        year+')',
                        debug)
                continue  # Al no haber año, no seguir buscando

            pathFTP_y = '/CF_UPMB/PQZIPDATA_'  # Linea para medidor G44X0
            pathOS_y = str(os.getcwd())

            logSave(logFile, "Carpetas de años tomados", debug)
            # Teniendo todas las carpetas de años que coinciden

            for w in range(0, len(foundYear)):
                # Entrar en el año w de la lista FTP
                ftp.cwd(pathFTP_y + '/' + foundYear[w])
                # Entrar en el año w de la lista OS
                # Si no existe, lo debes generar
                if not os.path.exists(pathOS_y+'\\'+foundYear[w]):
                    os.mkdir(pathOS_y+'\\'+foundYear[w])

                os.chdir(pathOS_y+'\\'+foundYear[w])

                mfolders = []
                mfolders = ftp.nlst()
                foundMonth = []
                for m in range(0, len(mfolders)):
                    if mfolders[m].find(month) == 0:  # Si mes coincide
                        foundMonth.append(mfolders[m])  # Guardalo

                if foundMonth == []:  # Si no hay folder de mes buscadi
                    logSave(logFile,
                            'El mes actual ('+month +
                            ') no existe en el medidor para el año ' +
                            str(foundYear[w]),
                            debug)
                    continue  # Continua con la siguiente

                logSave(logFile,
                        "Carpetas del mes encontradas",
                        debug)
                # print(str(foundMonth))  # Imprimir para ver los coincidentes

                pathFTP_m = pathFTP_y+'/'+foundYear[w]  # Linea para G44X0
                pathOS_m = str(os.getcwd())  # Path antes de mes local
        # ¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬
                for x in range(0, len(foundMonth)):
                    # Entrar en el mes x de la lista FTP
                    ftp.cwd(pathFTP_m+'/'+foundMonth[x])
                    # Entrar en el mes x de la lista local
                    # Si no existe, lo debes generar
                    if not os.path.exists(pathOS_m+'\\'+foundMonth[x]):
                        os.mkdir(pathOS_m+'\\'+foundMonth[x])

                    os.chdir(pathOS_m+'\\'+foundMonth[x])
                    # Dia dentro del mes
                    dfolders = []
                    dfolders = ftp.nlst()
                    # Revisa la existencia del dia biscado en los folders
                    d = pickDay(day, dfolders)
                    # Dentro de esta se mueve dentro al dia en FTP y OS
                    # Si el dia buscado no existe
                    if not d[:1] == day and not d[:2] == day:
                        logSave(logFile,
                                'El dia actual ('+day +
                                ') no existe en el medidor para el mes ' +
                                str(foundMonth[x]),
                                debug)
                        print('Continuando...')
                        continue

                    # Guardamos nombres de archivos
                    lista = []
                    lista = ftp.nlst()
                    # Adquirir los archivos
                    logSave(logFile,
                            "Guardando datos del mes "+foundMonth[x] +
                            ", dia "+d+".",
                            debug)
                    for z in lista:
                        try:
                            z2 = str(z)
                            valid = z2.split('.')[1]
                        except Exception:
                            continue
                        # Solo extraer .PQZip
                        if valid == 'PQZip':
                            # Z: archivo que se esta descargando
                            print(str(z))
                            filedata = open(z, 'wb')
                            ftp.retrbinary('RETR '+z, filedata.write)
                            filedata.close()
                    logSave(logFile,
                            "Datos del año "+foundYear[w] +
                            " mes "+foundMonth[x] +
                            ", dia "+d+", guardados!!",
                            debug)

            # Regresamos al folder de trabajo
            os.chdir(temp_a)

            # Cerramos ftp
            ftp.quit()
            logSave(logFile,
                    "Operacion terminada. Conexion cerrada para el host " +
                    HOST,
                    debug)

        logSave(logFile,
                'Transferencia de archivos completada' +
                ' para todos los medidores listados',
                debug)
