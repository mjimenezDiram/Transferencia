"""
Created on Tue Aug 11 12:18:52 2020

@author: lvazquez

Procedimiento de extraccion de PQZs para todos los PureBB's'

"""
import os.path
from ftplib import FTP
import datetime
import paramiko

from G44X0s import G44X0PQZs

class PureBBPQZs:

    def RecoverProcess(ips, logFile, debug, folderDict, clientesDict,
                       plantasDict):

        def pickDay(day, dfolders, connType):
            '''
            Function that covers the day folder search task.
            Also moves the program to that day folder.

            Parameters
            ----------
            day : string, Required
                DESCRIPTION. Day to looked for in the equipment for extraction.
            dfolders : list
                DESCRIPTION. List of day folders in the equipment.
            connType : string
                DESCRIPTION. Connection type (FTP or SFTP).

            Returns
            -------
            d : string
                DESCRIPTION. Name of the folder that matches the day needed.

            '''
            for d in dfolders:
                if d.find(day) == 0:
                    if connType == 'FTP':
                        ftp.cwd(d)
                        print('Folder actual: '+str(ftp.pwd()))
                    elif connType == 'SFTP':
                        SFTP.chdir(d)
                        print('Folder actual: '+str(SFTP.getcwd()))

                    if not os.path.exists(d):
                        os.mkdir(d)
                    os.chdir(d)
                    break
            return d

        def logSave(logFile, m, debug):
            '''
            Function to simplify the log writing process.
            Organizes the message, date and hour to generate a log like string.

            Parameters
            ----------
            logFile : TextIOWrapper, Required
                DESCRIPTION. Text Document Reference..
            m : string, Required
                DESCRIPTION. Message that is going to be written into the Log
                File.
            debug : str, optional
                DESCRIPTION. The default is '1'. Varible used to determine if
                the messages are going to be writeen into the text file.

            Returns
            -------
            None.

            '''
            NOW = datetime.datetime.now()  # Tiempo exacto actual
            NOW_log = str(NOW.strftime('[%Y'+'-'+'%m'+'-'+'%d]' +
                                       '[%H:'+'%M:'+'%S]'))
            # Tiempo en formato deseado
            log = NOW_log+'~'+m
            if debug == '1':
                logFile.write(log+'\n')
            print(log)

        def pathIdentifier(pathlocal, pathremoto, HOST, clientesDict,
                           plantasDict, folderDict, year, month, day,
                           debug, connType, temp_a, ftp='Error', SFTP='Error'):
            '''

            The function identifies the path, in the local server and the
            remote equipment, to do the file extraction. Also creates the
            folder Tree to organize files according to client, facility,
            Sapphire folder, Year, Month and day.
            Contains an inner function that copies the files.

            Parameters
            ----------
            pathlocal : string
                DESCRIPTION. Local server root Path where the PQZ folder tree
                starts.
            pathremoto : string
                DESCRIPTION. Equipment root path.
            HOST : string
                DESCRIPTION. Equipment IP direction taken from a list of active
                measurement equipments.
            clientesDict : Dictionary
                DESCRIPTION. Client dictionary that contains the relation
                client-IP direction.
            plantasDict : Dictionary
                DESCRIPTION. Facility dictionary that contains the relation
                Facility-IP direction.
            folderDict : Dictionary.
                DESCRIPTION. Folder name dictionary that contains the relation
                Sapphhire_Folder_Name-IP direction
            year : string
                DESCRIPTION. Year used to look for year folders in equipment
                system.
            month : string
                DESCRIPTION. Month number used to look for year folders in
                equipment system.
            day : string
                DESCRIPTION. Day number used to look for year folders in
                equipment system.
            debug : string
                DESCRIPTION. Variable used to define wheter the log messages
                are written to the logFile or not.
            connType : string
                DESCRIPTION. Conecction type. Defines the commands used in the
                equipment system.
            temp_a : string
                DESCRIPTION. Memory of the local server root folder.
            ftp : FTP, optional
                DESCRIPTION. The default is 'Error'. FTP connection reference.
                if this connection it's not possible, the variable won't
                contain the reference, but a string.
            SFTP : SFTP, optional
                DESCRIPTION. The default is 'Error'. SFTP connection reference.
                if this connection it's not possible, the variable won't
                contain the reference, but a string.

            Returns
            -------
            None.

            '''
            # Es necesario crear una carpeta para cada medidor
            os.chdir(pathlocal)  # Moverse a pathlocal en OS

            if connType == 'FTP':
                ftp.cwd(pathremoto)  # Moverse a PQZ/PQZDA_ en FTP(medidor)
            elif connType == 'SFTP':
                SFTP.chdir(pathremoto)

            # Localmente debo revisar si existe una carpeta del medidor
            # Sino, crearla
            if not os.path.exists(pathlocal+'\\'+clientesDict[HOST]+'\\' +
                                  plantasDict[HOST]):
                os.makedirs(pathlocal+'\\'+clientesDict[HOST]+'\\' +
                            plantasDict[HOST])

            pathlocal = (pathlocal+'\\'+clientesDict[HOST]+'\\' +
                         plantasDict[HOST])
            os.chdir(pathlocal)

            if not os.path.exists(pathlocal + '\\' + folderDict[HOST]):
                os.mkdir(pathlocal + '\\' + folderDict[HOST])

            pathlocal = (pathlocal+'\\' + folderDict[HOST])

            os.chdir(pathlocal)

            if not os.path.exists(pathlocal+'\\'+'\\PQZ\\'):
                os.mkdir(pathlocal+'\\'+'PQZ\\')

            pathlocal = pathlocal + '\\PQZ\\'
            os.chdir(pathlocal)

            # Path de la ip que se esta manejando
            pathip = pathlocal  # + '\\' + folderDict[HOST] + '\\PQZ\\'
            os.chdir(pathip)  # Moverse a la carpeta
    # ¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬
            # Año
            yfolders = []

            if connType == 'FTP':
                yfolders = ftp.nlst()
            elif connType == 'SFTP':
                yfolders = SFTP.listdir()

            foundYear = []
            for y in range(0, len(yfolders)):
                if yfolders[y].find(year) == 0:  # El año coincide
                    foundYear.append(yfolders[y])  # Guardalo

            if foundYear == []:
                logSave(logFile, 'No se encontro una carpeta para el año' +
                        ' buscado ('+year+')', debug)
                return  # Al no haber año, no tiene caso seguir buscando

            if connType == 'FTP':
                pathFTP_y = str(ftp.pwd())  # Path antes del año en el medidor
            elif connType == 'SFTP':
                pathFTP_y = str(SFTP.getcwd())

            pathOS_y = str(os.getcwd())  # Path del año en la carpeta local

            # Teniendo todas las carpetas de años que coinciden

            for w in range(0, len(foundYear)):
                # Entrar en el año w de la lista del medidor
                if connType == 'FTP':
                    ftp.cwd(pathFTP_y + '/' + foundYear[w])
                elif connType == 'SFTP':
                    SFTP.chdir(pathFTP_y + '/' + foundYear[w])

                # Entrar en el año w de la lista OS
                # Si no existe, lo debes generar
                if not os.path.exists(pathOS_y+'\\'+foundYear[w]):
                    os.mkdir(pathOS_y+'\\'+foundYear[w])

                os.chdir(pathOS_y+'\\'+foundYear[w])

                mfolders = []

                if connType == 'FTP':
                    mfolders = ftp.nlst()
                elif connType == 'SFTP':
                    mfolders = SFTP.listdir()

                foundMonth = []
                for m in range(0, len(mfolders)):
                    if mfolders[m].find(month) == 0:  # Si mes coincide
                        foundMonth.append(mfolders[m])  # Guardalo

                if foundMonth == []:  # Si no se encuentra mes busacado
                    logSave(logFile, 'El mes actual ('+month +
                            ') no existe en el medidor para el año ' +
                            str(foundYear[w]), debug)
                    continue  # Deten la busqueda para este año y continua

                if connType == 'FTP':
                    pathFTP_m = str(ftp.pwd())  # Path antes del mes en medidor
                elif connType == 'SFTP':
                    pathFTP_m = str(SFTP.getcwd())

                pathOS_m = str(os.getcwd())  # Path antes del mes en local
        # ¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬
                for x in range(0, len(foundMonth)):

                    # Entrar en el mes x de la lista del medidor
                    if connType == 'FTP':
                        ftp.cwd(pathFTP_m+'/'+foundMonth[x])
                    elif connType == 'SFTP':
                        SFTP.chdir(pathFTP_m+'/'+foundMonth[x])

                    # Entrar en el mes x de la lista local
                    # Si no existe, lo debes generar
                    if not os.path.exists(pathOS_m+'\\'+foundMonth[x]):
                        os.mkdir(pathOS_m+'\\'+foundMonth[x])

                    os.chdir(pathOS_m+'\\'+foundMonth[x])

                    # Dia dentro del mes
                    dfolders = []

                    if connType == 'FTP':
                        dfolders = ftp.nlst()
                    elif connType == 'SFTP':
                        dfolders = SFTP.listdir()
                    # Revisa carpetas de dias por existencia del buscado.
                    print('dfolders:\n')
                    print(dfolders)
                    d = pickDay(day, dfolders, connType)
                    # Dentro de esta se mueve dentro al dia en FTP y OS
                    print('Valor de d:' + str(d))

                    if not d[:2] == day:  # Si no existe dia buscado
                        logSave(logFile, 'El dia actual (' + day +
                                ') no existe en el medidor para el mes ' +
                                str(foundMonth[x]), debug)
                        continue

                    # Guardamos nombres de archivos encontrados en medidor
                    lista = []

                    if connType == 'FTP':
                        lista = ftp.nlst()
                    elif connType == 'SFTP':
                        lista = SFTP.listdir()

                    # Adquirir los archivos
                    logSave(logFile, "Guardando datos del mes " +
                            foundMonth[x]+", dia "+d+'.', debug)

                    fileCopy(lista, d, connType)

                    logSave(logFile, "Datos del año " + foundYear[w] +
                            " mes "+foundMonth[x] +
                            ", dia "+d +
                            ", guardados!4", debug)

            # Regresamos al folder de trabajo
            os.chdir(temp_a)

            # Cerramos conexion
            if connType == 'FTP':
                ftp.quit()
            elif connType == 'SFTP':
                SFTP.close()
                SSH.close()

            logSave(logFile, "Operacion terminada." +
                    " Conexion cerrada para el host "+HOST, debug)

        def fileCopy(lista, d, connType):
            '''
            Function that covers the file copy task, depending on the
            connection type and the list of files.

            Parameters
            ----------
            lista : List
                DESCRIPTION. List of files to be copied
            d : string
                DESCRIPTION. Day folder name where the file is going to be
                copied.
            connType : String
                DESCRIPTION. Connection type that defines the commands used to
                copy the file.

            Returns
            -------
            None.

            '''
            for z in lista:
                # Verificacion de extension
                ext = z.split('.')[1]
                if ext == 'pqz':
                    print(z)
                    try:
                        if connType == 'FTP':
                            filedata = open(z, 'wb')
                            ftp.retrbinary('RETR '+z, filedata.write)
                            filedata.close()
                        elif connType == 'SFTP':
                            SFTP.get(z, str(os.getcwd()) + '\\' + z)
                    except Exception:
                        logSave(logFile, "Problema al trasladar archivo" +
                                '\n', debug)
                        continue

        # ¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬MAIN¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬
        print('Medidor PureBB')
        port = 21
        ftp = ''
        SFTP = ''
        # Folder de trabajo inicial
        temp_a = os.getcwd()
        # Registramos la fecha a descargar (Default: dia anterior)
        today = datetime.date.today()
        oneday = datetime.timedelta(days=1)
        yesterday = today-oneday
        yesterday = yesterday-oneday
        yesterday = yesterday-oneday
        year = str(yesterday.year)
        month = str(yesterday.month)
        if len(month) == 1:
            month = "0"+month
        day = str(yesterday.day)

        if len(day) == 1:
            day = "0"+day
        host = ips
        logSave(logFile, 'Parametros de fecha definidos', debug)
        # ¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬
        for HOST in host:  # Para cada IP diferente

            # Diccionario de folders iniciales
            pathIniDict = {'pathIniDev': 'Z:\\CLOUDPQDev\\MedicionRemota',
                           'pathIniProd': 'Z:\\CLOUDPQProd\\MedicionRemota',
                           'pathIniLocal': 'C:\\Users\\mjimenez\\Documents\\MedicionRemota\\'}
                           #'pathIniLocal': 'C:\\Users\\mjimenez\\OneDrive - DIRAM SA DE CV\\Escritorio\\+'# C:\\Users\\mjimenez\\Desktop\\' +
                           #'PruebasPQZ'}

            logSave(logFile, "Estableciendo conexion FTP para el host " +
                    HOST+"...", debug)
            # Conexion FTP
            try:
                ftp = FTP()
                ftp.connect(HOST, port)
                ftp.login('user0', 'aA123123')
                connType = 'FTP'
                logSave(logFile, "Conexion FTP establecida para el host " +
                        HOST+'\n', debug)
            except Exception:
                # Informa de fallo en conexion FTP
                logSave(logFile, 'Conexión FTP con el host '+HOST +
                        ' no establecida2', debug)
                try:
                    # Informa de intento de conexion SFTP
                    logSave(logFile, 'Intentando conexion SFTP para host: ' +
                            HOST, debug)
                    # Conexion SFTP
                    SSH = paramiko.SSHClient()
                    SSH.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    SSH.connect(HOST, username='user0', password='aA123123')

                    SFTP = SSH.open_sftp()  # Abrir SFTP
                    connType = 'SFTP'
                    logSave(logFile, "Conexion SFTP establecida" +
                            " para el host " + HOST+'\n', debug)
                    
                   

                except Exception:
                    # Si tampoco se puede por SFTP, continua con otro medidor
                    logSave(logFile, 'Conexion SFTP con el host '+HOST +
                            ' no establecida3', debug)
                    
                    ipsG4 = []
                    ExtraccionG4 = G44X0PQZs
                    ipsG4.append(HOST)
                    ExtraccionG4.RecoverProcess(ipsG4, logFile, debug, folderDict, clientesDict, plantasDict)
                    continue

            # pathlocal = pathIniDict['pathIniDev']
            # pathlocal = pathIniDict['pathIniProd']
            pathlocal = pathIniDict['pathIniLocal']
            pathremoto = 'PQZ/PQZDA_'
            #pathremoto = 'CF_UPMB/PQZIPDATA_'  # Linea para medidor G44X0    MJF 20230615 cambiar para cuando es G44XX
            # Funcion generadora de paths para cada medicion
            pathIdentifier(pathlocal, pathremoto, HOST, clientesDict,
                           plantasDict, folderDict, year, month, day,
                           debug, connType, temp_a, ftp, SFTP)

        logSave(logFile, 'Transferencia de archivos completada' +
                ' para todos los medidores listados', debug)
