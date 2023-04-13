
<!-- README.md is generated from README.Rmd. Please edit that file -->

# zooenv

<!-- badges: start -->
<!-- badges: end -->

Package in construction . Just set to be usable. Using this package
require a password and username to access the SGDO Oracle database at
MPO -Québec. This package allows for merging of environmental variables
to zooplankton data by date and position and computes average over
particular depth (temperature 0-50m for exemple).

This package requires ROracle. To work properly on DFO computers follow
this sequence for installation: 1. Installer R mais pas dans Program
files. Choisir le C comme répertoire 2. Installer RStudio 3. Installer
Rtools avec la version qui correspond à votre version de R
<https://cran.r-project.org/bin/windows/Rtools/> pour la version de
Rtools4.0, suivre les instruction en bas pour un fichier .environ
(utilise writeLines de préférence). À partir de Rtools 42 ou R.4.2 le
path est attribué automatiquement si vous suivez bien les indications
sur les emplacements d’installation des programmes. 4. Installer
l’instant client et le package sdk. =Télécharger et dezipper les 2 dans
C. sdk va aller se coller dans l’instant client.
<https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html>
5. Copier les fichiers sdk/include d’Oracle dans le include de R (C:.2
). Pour que ça fonctionne, il ne faut pas que R soit installer dans
Program files parce que ça prend les droits d’admin pour y écrire.

6.  Dans R utiliser les commandes suivantes :
    Sys.setenv(OCI_LIB64=“C:/instantclient_19_8”) selon votre version
    bien entendu.
    Sys.setenv(OCI_INC=“C:/instantclient_19_8/sdk/include”)
7.  Installer ROracle de CRAN. Quand on demande si vous voulez le «
    compile from source » =oui!

Credentials to access SGDO are stored in options and can be set by
loading a RProfile or by using the function set_sgdo_pass which pops
windows for entering your credentials.
