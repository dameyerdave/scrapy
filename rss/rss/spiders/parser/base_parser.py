class BaseParser:
    def process(self, response):
        item = response.meta['item'];
        for meta in response.css('meta[property]'):
            item[meta.css('::attr(property)').extract_first()] = meta.css('::attr(content)').extract_first()
        for meta in response.css('meta[itemprop]'):
            item[meta.css('::attr(itemprop)').extract_first()] = meta.css('::attr(content)').extract_first()
        for meta in response.css('meta[name]'):
            item[meta.css('::attr(name)').extract_first()] = meta.css('::attr(content)').extract_first()
        item.image = response.css('meta[property="og:image"]::attr(content)').extract_first()
        return self.parse(response);

    def parse(self, response):
        raise NotImplementedError("Please Implement parse(self, response)")
