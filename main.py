from Google import Create_Service
import pandas as pd


CLIENT_SECRET_FILE = 'credentials.json'
API_SERVICE_NAME = 'drive'
API_VERSION = 'v3'
SCOPES = ['https://www.googleapis.com/auth/drive']
service = Create_Service(CLIENT_SECRET_FILE, API_SERVICE_NAME, API_VERSION, SCOPES)

folder_path = r"D:\FoldersCreated\All Data\FolderNames.xlsx"
df = pd.read_excel(folder_path)
folder_names = list(df['Campaigns Name'])
folder_reso = list(df['reso'])
map = zip(folder_names , folder_reso)
mapped = list(map)
print(f"Campaign Names to be created--: {folder_names}\n")



sub_folder = r"D:\FoldersCreated\All Data\SubFolders.xlsx"
df = pd.read_excel(sub_folder)
sub_folder_names = list(df['SubFolders'])

inside_SubFolders = r"D:\FoldersCreated\All Data\InsideSubFolders.xlsx"
df = pd.read_excel(inside_SubFolders)
more_sub_folders = list(df['SubFolderss'])

shared_emails = r"D:\FoldersCreated\All Data\sharedEmailsAll.xlsx"
df = pd.read_excel(shared_emails)
emails = list(df['EMAIL'])
print(f"List of EMails to be--: {emails}\n\n\n")
final_list = {}


def creating_Sub_folders_inside_qc(id,more_sub_folders):
    for more_sub_folder in more_sub_folders:
        sub_metadata = {
            'name': more_sub_folder,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [sub_ids]}  # Passing folder id for QC_public folder
        print(f"***SubFolders--> {more_sub_folder}  $$$CREATED IN QC$$$ ")
        service.files().create(body=sub_metadata).execute()
        continue

for folder_name, reso in mapped:
    print('--' * 75)
    print(f"Working for ---> {folder_name}")
    na_reso = "**********" #Parent folder ID
    eu_reso = "**********" #Another parent folder ID
    if reso == "Resources":
        reso_id = na_reso
    else:
        reso_id = eu_reso
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [reso_id]}  #  "0ByPTclWdpc0KYjFxY2lrLWtrYTQ"This is MAIN resource or NOW(EU=-Resource folder id" 1ydVoRQvoVfv0S7euVUOF9sW1c8OlPA3Q "
    file = service.files().create(body=file_metadata).execute()  # this is for creating Campaign folders
    resource=service.files()
    resultss = service.files().list(q=f"name='{folder_name}'", fields="files(id, name)").execute()
    files = resultss.get('files')
    #print(files)

    parent_name = files[0]['name']
    id = files[0]['id']
    folder_id = id      # Taking folder id of campaigns all at once
    print(f"{folder_name}  ***Created and its Id*** ---->  {id}")
    for sub_folders in sub_folder_names:
        sub_metadata = {
            'name': sub_folders,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [folder_id]}   # Passing folder id here one by one to Create subfolders inside
        create_subfolder = service.files().create(body=sub_metadata).execute()  # this is Creating 1st subfolder
        print(f"***SubFolders--> {sub_folders}  $$$CREATED$$$")

    query = f"parents = '{folder_id}'"
    response = service.files().list(pageSize=1000, q=query).execute()
    files = response.get('files')
    nextPageToken = response.get('nextPageToken')
    #print(files)
    while nextPageToken:
        response = service.files().list(pageSize=1000, pageToken=nextPageToken, q=query).execute()
        files.extend(response.get('files'))
        nextPageToken = response.get('nextPageToken')

    try:
        print('\n')  # ------------finding subid for QC_public---------------------
        for i in files:
            if 'QC_Public' in i['name']:
                sub_id = i['id']
                #file_id = sub_id
                child_name = i['name']
                #print(child_name)
                print(f"***Searching for QC_Public in ----> {folder_name}: {i['name']}")
                print(f"***ID of QC_Public ---> {sub_id}" ' <-----QC_folder Found...Creating subfolders***')
                renaming_sub_folder = child_name + ' ' + '(' + parent_name + ')'
                print(f"***Sub folder new name--->: {renaming_sub_folder}")
                sub_ids = sub_id
                file = {'name': renaming_sub_folder}  # renaming QC folder
                updated_file = resource.update(
                    fileId=sub_ids,
                    body=file,
                    fields='name').execute()  # ###renaming QC folder

                for email in emails:
                    def callback(request_id, response, exception):
                        if exception:
                            # Handle error
                            print
                            exception
                        else:
                            print
                            "Permission Id: %s" % response.get('id')


                    batch = service.new_batch_http_request(callback=callback)

                    domain_permission = {
                        'type': 'user',
                        'role': 'reader',
                        'emailAddress': email
                    }
                    batch.add(service.permissions().create(
                        fileId=sub_ids,
                        body=domain_permission,
                        fields='id',
                        sendNotificationEmail=False
                    ))
                    batch.execute()
                    print(f"Sharing to email:{email}")

        create = creating_Sub_folders_inside_qc(sub_ids,more_sub_folders)
        print("\n")
        print(f"***Folder creating done for ---> $$  {folder_name} $$")
        print("\n"*2)


    except:
        print("#" * 10 + f"Some Error in finding QC and creating Subfolder {folder_name}" + '#' * 10)
        final_list[folder_name] = "failed"
        continue



print("\n")
print(f"List of Failed Campaigns ---> {final_list}")
print("$"*40, "Process executed", "$"*40)
