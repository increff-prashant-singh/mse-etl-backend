class QueryUserData:
    def __init__(self, status=False, message="", id=0, username="", email="", fullName="", domainName="",
                 appName="", domainId=0, country="", roles=[], authMode="", phone="", orgName="",
                 resourceRoles={}, *args, **kwargs):
        self._status = status
        self._message = message
        self._id = id
        self._username = username
        self._email = email
        self._fullName = fullName
        self._domainName = domainName
        self._appName = appName
        self._domainId = domainId
        self._country = country
        self._roles = roles
        self._authMode = authMode
        self._phone = phone
        self._orgName = orgName
        self._resourceRoles = resourceRoles

    # Getter and setter for 'status'
    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    @property
    def fullName(self):
        return self._fullName

    @fullName.setter
    def fullName(self, value):
        self._fullName = value

    @property
    def email(self):
        return self._email

    @email.setter
    def email(self, value):
        self._email = value

    @property
    def username(self):
        return self._username

    @username.setter
    def username(self, value):
        self._username = value

    # Getter and setter for 'message'
    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, value):
        self._message = value

    # Define getters and setters for other attributes similarly

    # For list attributes (roles)
    @property
    def roles(self):
        return self._roles

    @roles.setter
    def roles(self, value):
        self._roles = value

    # For dictionary attributes (resourceRoles)
    @property
    def resourceRoles(self):
        return self._resourceRoles

    @resourceRoles.setter
    def resourceRoles(self, value):
        self._resourceRoles = value


class QueryTokenData:
    def __init__(self, successful=False, token="", message="", *args, **kwargs):
        self.successful = successful
        self.token = token
        self.message = message

    # Getter and setter for 'successful'
    @property
    def successful(self):
        return self._successful

    @successful.setter
    def successful(self, value):
        self._successful = value

    # Getter and setter for 'token'
    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, value):
        self._token = value

    # Getter and setter for 'message'
    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, value):
        self._message = value


class QueryUserForm:
    def __init__(self,*args, **kwargs):
        self.domainName = None
        self.username = None

    def set_domainName(self, domainName):
        self.domainName = domainName

    def set_username(self, username):
        self.username = username

    def get_domainName(self):
        return self.domainName

    def get_username(self):
        return self.username