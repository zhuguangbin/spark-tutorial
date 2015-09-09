__author__ = 'zhugb'

def toNormalCookie(cookiebytes):
    if not isinstance(cookiebytes,list):
        return "???????????????????????"
    elif len(cookiebytes) != 12:
        return "???????????????????????"
    else:
        firstPart = [ str((i & 0xF0) >> 4) + str(i & 0x0F) for i in cookiebytes[:11]]
        print firstPart
        firstPartStr = "".join(firstPart)
        lastPartStr = str((cookiebytes[11] & 0xFF) >> 4)
        return firstPartStr+lastPartStr


