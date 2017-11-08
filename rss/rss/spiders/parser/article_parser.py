from base_parser import BaseParser

class ArticleParser(BaseParser):
    def __init__(self, contentCss):
        self.contentCss = contentCss

    def parse(self, response):
        item = response.meta['item'];
        item.type = 'article';
        content = response.css(self.contentCss).extract()
        if len(content) > 10:
            for cp in content:
                if 'content' in item:
                    if len(item.content) > 0:
                        item.content += ' '
                    item.content += cp
                else:
                    item.content = cp
            yield item
        else:
            with open('nocontent.log', 'a') as f:
                f.write(item.reference + '\n')
