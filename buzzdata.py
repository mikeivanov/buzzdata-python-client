import json
import requests

class Buzzdata(object):
    """
    Buzzdata API Client.
    """
    
    class Error(Exception):
        def __init__(self, response):
            self.code = response.status_code
            json = response.json
            if json:
                self.message = json['message']
            else:
                self.message = response.text

        def __str__(self):
            return "Buzzdata API Error: %s (%r)" % (self.message, self.code)

    def __init__(self, access_token=None, base_url="https://buzzdata.com"):
        """
        Create a new client instance.
        
        **Parameters**:

        :param base_url: a hive server's base URL, such as
                         'https://buzzdata.com' (default),
                         'https://myhive.buzzdata.com' and so on.

        :access_token:   an OAuth2 access token.
                         If not present, most of the calls will raise
                         Invalid OAuth Request.

        **Obtaining an OAuth2 token**

        OAuth2 interactions are out of the scope of this module. You can do
        something like this to get a token:
        
            >>> from requests_oauth2 import OAuth2
            >>> def authorize_url(consumer_key, consumer_secret,
            ...                   base_url='https://buzzdata.com',
            ...                   scope='api_access',
            ...                   redirect_uri='http://localhost:8080/foobar'):
            ...     handler = OAuth2(consumer_key, consumer_secret,
            ...                      base_url, redirect_uri)
            ...     return handler.authorize_url(scope, response_type='token')
            >>> print authorize_url('<key>', '<secret>')
            https://buzzdata.com/oauth/authorize?response_type=token&...

        Copy the printed URL and paste it into the browser. Confirm
        authorization. When redirected, copy the token and somehow paste into
        your client. It's a good idea to use a local http server or a service
        like http://requestb.in/ for intercepting token redirects.
        """
        self.api_url = base_url + '/api'
        if access_token is None:
            params = None
        else:
            params = {'access_token': access_token}
        self.client = requests.session(params=params)

    # general info
    
    def licenses(self):
        return self._get("licenses")

    def topics(self):
        return self._get("topics")

    def search(self, query):
        return self._get("search", term=query)

    # users
    
    def user_info(self, username):
        return self._get(username)['user']

    def create_user(self, username, email, password):
        return self._post("users", **form('user',
                                          username=username,
                                          email=email,
                                          password=password))

    # datarooms

    def list_datarooms(self, username):
        return self._get("%s/datasets/list" % username)

    def create_dataroom(self, username, name, readme="", license="cc0",
                        topics=[], public=False):
        result = self._post("%s/datasets" % username,
                            **form('dataset',
                                   name=name,
                                   readme=readme,
                                   license=license,
                                   topics=list(topics),
                                   public=public))
        return result['dataset']

    def dataroom_overview(self, dataroom_id):
        return self._get(dataroom_id)['dataset']

    def delete_dataroom(self, dataroom_id):
        return self._delete(dataroom_id)

    # visualizations

    def list_visualizations(self, dataroom_id):
        return [dict(vis, id="%s/visualizations/%s" % (dataroom_id, vis['uuid']))
                for vis in self._get("%s/visualizations" % dataroom_id)]

    def create_visualization_from_url(self, dataroom_id, url, title=""):
        return self._post("%s/visualizations/url" % dataroom_id,
                          url=url,
                          title=title)

    def create_visualization_from_image(self, dataroom_id,
                                        image_file, file_name, title=""):
        return self._post("%s/visualizations/image" % dataroom_id,
                          files=dict(image=(file_name, image_file)),
                          title=title)

    def delete_visualization(self, visualization_id):
        return self._delete(visualization_id)

    # datafiles

    def list_datafiles(self, dataroom_id):
        return self._get("%s/list_datafiles" % dataroom_id)

    def create_datafile(self, dataroom_id, datafile_name):
        result = self._post("%s/create_datafile" % dataroom_id,
                            data_file_name=datafile_name)
        return (dataroom_id, result['datafile_uuid'])

    def datafile_history(self, datafile_id):
        return self._get("data_files/%s/history" % datafile_id[1])

    def new_upload_request(self, datafile_id):
        result = self._post("%s/upload_request?datafile_uuid=%s" % datafile_id)
        return result['upload_request']

    def upload_datafile(self, datafile_id, file, file_name, release_notes=""):
        upload_request = self.new_upload_request(datafile_id)

        # Prepare our request
        post_url = upload_request.pop('url')
        upload_request['release_notes'] = release_notes
        return requests.post(post_url,
                             files={'file': (file_name, file)},
                             data=upload_request)

    def get_download_url(self, datafile_id, version=None, type='CSV'):
        type = type.upper()
        if type not in ('CSV', 'XLS', 'XLSX'):
            raise ValueError("Unknown file type '%s'" % type)
        result = self.post_json("%s/%s/download_request" % datafile_id,
                                type=type,
                                version=version)
        return result['download_request']['url']

    def download_data(self, datafile_id):
        return requests.get(self.get_download_url(datafile_id))

    # staging

    def create_stage(self, datafile_id):
        result = self._post("%s/%s/stage" % datafile_id)
        return datafile_id + (result['id'],)

    def insert_rows(self, stage_id, rows):
        return self._post("%s/%s/stage/%s/rows" % stage_id,
                          rows=json.dumps(rows))

    def update_row(self, stage_id, row_number, row):
        return self._put("%s/%s/stage/%s/rows/%d" % (stage_id + (row_number,)),
                         row=json.dumps(row))

    def delete_row(self, stage_id, row_number):
        return self._delete("%s/%s/stage/%s/rows/%d" % (stage_id + (row_number,)))

    def commit_stage(self, stage_id):
        return self._post("%s/%s/stage/%s/commit" % stage_id)

    def rollback_stage(self, stage_id):
        return self._post("%s/%s/stage/%s/rollback" % stage_id)
  
    # private
    
    def _request(self, method, path, params, data=None, files=None):
        response = method(self.api_url + '/' + path,
                          data=data,
                          files=files)
        if response.status_code > 400:
            raise Buzzdata.Error(response)
        return response.json

    def _get(self, path, **params):
       return self._request(self.client.get, path, params)
    
    def _delete(self, path, **params):
       return self._request(self.client.delete, path, params)
    
    def _put(self, path, **data):
       return self._request(self.client.put, path, {}, data=data)
    
    def _post(self, path, files=None, **data):
       return self._request(self.client.post, path, {}, data=data, files=files)
    

def form(formname, **fields):
    return dict(('%s[%s]' % (formname, fieldname), value)
                for fieldname, value in fields.items())

