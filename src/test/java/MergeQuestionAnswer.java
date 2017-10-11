import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

import net.sf.json.JSONObject;

public class MergeQuestionAnswer {
	public static void main(String[] args){
		/* 口味 */
		String taste = "酸,甜,苦,辣,咸,湿,软,硬,肥,瘦,腻,清口";
		/* 烹饪时间 */
		String time = "分,小时,一会,片刻,半天,整天,多久";
		/* 食品评价 */
		String comments = "好吃,难吃,有营养,没营养,高脂肪,低脂肪,健康,不健康,焦";
		/* 烹饪方法 */
		String method = "炒,煎,烹,油炸,溜,勾芡,煸,烩,焖,烧,扒,汆,涮,煮,炖,煨,焐,蒸,卤,烤,炝,腌,拌,拔丝,焗";
		/* 食材 */
		String material = "枣,草莓,蓝莓,黑莓,桑葚,覆盆,葡萄,青提,红提,葡萄,蜜橘,砂糖橘,金橘,蜜柑,甜橙,脐橙,西柚,葡萄柚,柠檬,文旦,莱姆,油桃,蟠桃,水蜜桃,黄桃,李,樱桃,杏,梅,杨梅,西梅,乌梅,枣沙,枣,海枣,蜜枣,橄榄,荔枝,龙眼,桂圆,槟榔,苹果,红富士,红星,国光,秦冠,黄元帅,梨,砂糖梨,黄金梨,莱阳梨,香梨,雪梨,香蕉梨,蛇果,海棠,沙果,柿,山竹,黑布林,枇杷,杨桃,山楂,圣女果,无花果,白果,罗汉果,火龙果,猕猴桃,西瓜,美人瓜,甜瓜,香瓜,黄河蜜,哈密瓜,木瓜,乳瓜,菠萝,芒果,栗子,椰子,奇异果,芭乐,榴莲,香蕉,甘蔗,百合,莲子,石榴,核桃,拐枣,"
				 + "西红柿,大白菜,小白菜,抱子甘蓝,羽衣甘蓝,紫甘蓝,结球甘蓝,生菜,菠菜,韭菜,芹菜,苦苣,油麦菜,黄秋葵,空心菜,茼蒿,苋菜,香椿,娃娃菜,芥兰,荠菜,香菜,茴香,马齿苋,木耳叶,芥菜,芜荽,大叶香菜,小叶香菜,雪里蕻,油菜,紫苏,黑芝麻,萝卜,白罗卜,胡罗卜,水罗卜,大葱,小葱,蒜,洋葱,莴笋,山药,马铃薯,红薯,卜留克,芦笋,石刁柏,牛蒡,百合,豌豆芽,香椿芽,萝卜芽,荞麦芽,花生芽,姜芽,黄豆芽,绿豆芽,白菜花,绿菜花,辣椒,菜椒,青椒,尖椒,甜椒,朝天椒,线椒,南瓜,金丝南瓜,黑皮冬瓜,苦瓜,黄瓜,丝瓜,佛手瓜,西葫芦,番茄,茄子,芸豆,豇豆,豌豆,架豆,刀豆,扁豆,菜豆,毛豆,蛇豆,甜玉米,木耳,银耳,平菇,草菇,金针菇,香菇,"
				 + "肉,牛肉,羊肉,猪肉,鸡肉,狗肉,鸭,鹅,兔肉,鹿,鸽子,鹌鹑,老鼠,蛇,蛙,田鸡,牛蛙,青蛙,"
				 + "汤,丸子,肉,肠,肚,臀尖,后颈,翅,蹄筋,口条,百叶,胗,"
				 + "小米,大米,豆,谷,高粱,玉米,杂粮,荞麦,麦,饭,粉,"
				 + "鱼,大黄鱼,雅片鱼,小嘴鱼,多宝鱼,海黑鱼,先生鱼,小姐鱼,海鳝鱼,海鲶鱼,海鲁鱼,海兔鱼,老板鱼,皮匠鱼,石浆鱼,美国红,象拔蚌,活大鲍,活小鲍,活海参,活海肠,活甲鱼,沙鱼,大鸦片,大海鳝,大鲁子,三文鱼,小嘴鱼,棒鱼,老板鱼,黄花鱼,安康鱼,拔鱼,加吉鱼,海浮鱼,八角鱼,沙丁鱼,红头鱼,大头鱼,昌鱼,尖头鱼,刀鱼,先生鱼,面条鱼,黑鱼,扔巴鱼,梭鱼,鲫鱼,红刀鱼,河刀鱼杂拌鱼,桂花鱼,大头宝,九兔鱼,胖头鱼,碟鱼头,金枪鱼,八带鱼,岛子鱼,同乐鱼,八爪鱼,乌鱼,夏威夷贝,红里罗,红扇宝,柽子王,大海螺,小海鲜,韩国螺,乌鲍螺,鸟贝壳,肚脐螺,天鹅蛋,芒果贝,白云贝,蝴蝶贝,百花贝,小姐贝,虎皮贝,红贝,龙眼贝,玻璃贝,毛鲜子,麻蚬子,海蛎壳,赤贝,北极贝,象拔榜,海红,毛海红,小柽子,笔杆柽,小海鲜,小人鲜,马蹄贝,黑牛眼,文蛤带子,赤子,小海波螺,香螺,香波螺,辣波螺,尖波螺,偏定螺,海兔,花螺,钢螺,青口贝,白蚬子,海螺丝,蜗牛螺,鲜紫菜,龙须菜,鹿脚菜,海嘛线,海带片,海木耳,海带扣,龙虾,龙虾仔,基围虾,皮虾,青虾,大海虾,卢姑虾竹节虾,桃花虾,小河虾,小红虾,鸟贝,大蛤,牡蛎,鲜贝丁,扇贝,沙鲜,黄鳝,海肠,毛蚬肉,尤鱼须,鲜鱼杂,鲜鱼肚,青鱼子,刀鱼子,沙鱼脑,蛰头,蛰皮,鲜海蛰,沙鱼肚,先生鱼,柽子嘴,功夫菜,水发参,虾仁,海狗鞭,鱼筋,鱼肠,鱼白,鱼肚,沙鱼皮,凌鱼皮,沙鱼喉,蚕蛹,雄蚕鹅,蟹黄,红鱼子,焖子";
				 
		/* 调味料 */
		String sauce = "老干妈,食盐,生抽,老抽,醋,料酒,白酒,甜面酱,豆瓣酱,豆豉,番茄酱,番茄沙司,芝麻酱,沙拉酱,白糖,冰糖,红糖,辣椒,麻椒,花椒,八角,香叶,桂皮,黑胡椒,白胡椒,孜然,小茴香,五香粉,咖喱,豆腐乳,剁椒,泡椒,淀粉,味精,鸡精,蚝油";
		/* 厨具 */
		String instrument = "锅,洗碗机,消毒柜,榨汁机,切削器,打蛋器,炉,灶,菜刀,剁骨刀,汤勺,漏勺,筷子,碗,盘,碟,叉子,餐刀,电饭锅,电磁炉,电饼铛,烤箱,烤架,砂锅,蒸锅,笼屉";
		/* 菜品 */
		String type = "卤肉,炒饭,炒面,炒肉,炒粉,烤鸭,红烧肉,烤鱼,烤肉,烤馍,泡菜,拌饭,拌面,板面,刀削面,凉皮,肉夹馍,烧鸭,涮羊肉,炒蛋,煎蛋,蛋炒饭,烤蛋,炖蛋,煮蛋,茶叶蛋,卤蛋,糖炒栗子,生煎,小笼包,锅贴,饺子,卤煮火烧,火烧,红烧牛肉,锅炖,涮锅,煎饼果子,冰糖葫芦,闷炉烤鸭,炒米粉,米粉,卤煮,烧鸭,烧鸡,烧鹅,叉烧,炖牛,黄焖鸡,黄焖鸡米饭,炒鸡蛋,煮鸡蛋,炒米饭,炒面条,煎鸡蛋,烤冷面,"
				+ "雪花鸡淖,火爆腰花,酸辣臊子蹄筋,炝黄瓜,麻酱凤尾,家常海参,鲜花豆腐,坛子肉,参麦团鱼,芹黄鱼丝,芪烧活鱼,鱼香肉片,叉烧鱼,清汤燕菜,复元汤,乌发汤,鸡包鱼翅,锅贴鸡片,椒盐八宝鸡,合川肉片,烩鸭四宝,鱼香肉丝,豆瓣鲫鱼,银杏蒸鸭,一品海参,爆炒腰花,金钱鸡塔,鱼香荷包蛋,金钱海参,绣球鱼翅,原笼玉簪,羊耳鸡塔,火爆荔枝腰,盐水肫花,荷包鱿鱼,沙参心肺汤,冬菜扣肉,酱酥桃仁,红油耳片,酱爆肉,粉蒸排骨,豆鼓鱼,鱼香腰花,炸珍珠虾,枸杞煨鸡汤,黄豆芽排骨豆腐汤,雪花鸡淖,火爆腰花,回锅肉,玉兔葵菜尖,南卤醉虾,荷包豆腐,宫保鸡丁,"
				+ "肉丝拉皮,五香肉干,芝麻肉丝,酱爆肉丁,水晶肘子,盐水大虾,水晶鸡脯,炒肚皮,糖醋里脊,红烧鱼翅,炒里脊丝,蒲棒里脊,芫爆里脊丝,爆炒肉片,清炸里脊,火爆燎肉,荷叶肉,粉蒸肉,金针肉,鸡里爆,麻辣鸡块,生菜拌鸭掌,宫爆鸡丁,炒辣子鸡丁,炒胗肝,清炸胗肝,炒鸡丝,炸八块,鸡丝蜇皮,炸鸡排,香酥鸡腿,芙蓉鸡片,锅鳎鸡签,红烧翅,白菇炖鸡,黄焖鸡块,卷筒鸡,碧桃鸡,栗子鸡,纸包鸡,烤鸭,酱爆烤鸭片,母子鸡,布袋鸡,清蒸鸡,油烹雏鸡,白松鸡,一卵孵双,黄焖鸭肝,炸面包鸭肝,酱汁鸭方,酱汁鸭方,炸芙蓉鸭子,清汤鸭条,神仙鸭子,油爆鲜贝,乌龙吐珠,盐爆海肠子,鸡丝蜇皮,清炖元鱼,木樨肉,炒全蟹,糖醋鲤鱼,煎转鲫鱼,干烧鲳鱼,煎转鲫鱼,糟煨桂鱼,醋椒活鱼,白汁酿鱼,蒲酥全鱼,红烧鱼,绣球全鱼,菊花全鱼,清蒸鲥鱼,烧荷包鲫鱼,番茄松鼠鱼,糟溜牡丹鱼,糖醋棒子鱼,醋烹黄花鱼,家常熬黄花鱼,糖醋金钱鱼,红烧元鱼,八宝元鱼,鸳鸯鱼扇,两吃鱼卷,油爆鱼芹,锅塌鱼片,溜鱼片,糟炒鱼片,番茄鱼片,滑炒鱼丝,炒鱿鱼丝,油爆乌鱼花,芫爆鱿鱼卷,爆双花,油爆海螺,炸鲜贝串,炸鲜贝串,赛螃蟹,炒全蟹,炒虾仁,腰丁虾仁,面包虾仁,鸡汁虾仁,番茄虾仁,清炒尾虾,樟茶鸭,煎烧虾段,醋烹虾段,大虾,炸雪丽大虾,三彩大虾,炸蝴蝶虾,百花大虾,炸菊花虾排,一品燕菜,芙蓉燕菜,百花燕菜,通天鱼翅,鱼翅四丝,红扒熊掌,鲍翅熊掌,红烧鹿筋,御笔猴头,云片猴头,扒酿猴头,糖醋鲤鱼,山东海参,红烧鹿筋,一品燕菜,绣球干贝,葫芦鱼翅,彩云鱼肚,蟹黄鱼肚,白扒鱼肚,白扒裙边,红扒鱼唇,扒鲍鱼芦笋,扒原壳鲍鱼,葱烧海参,虾籽烧海参,扒酿海参,清汤芙蓉哈什蟆油,炸赤鳞鱼,清汤蝴蝶竹荪,炸蛎黄,烧菊花牛鞭,红烧干贝,干贝四宝,干贝萝卜球,鸡汁干贝,纱窗明月汤,胡椒海参汤,沙锅趸皮,木樨汤,辣汤,三鲜汤,榨菜肉丝汤,清汤鱿鱼卷,汆虾蘑海,汆捶鸡片,清汆赤鳞鱼　,清汤蝴蝶海参,清汤怠耳鸭舌,清汤干贝鸡鸭腰,清汤鲍鱼,清汤全家福,清汤芙蓉黄管,清汤干贝菊花菇蟹,汤爆双脆,奶汤鸡脯,奶汤蒲菜,奶汤茭白,奶汤怠肺,奶汤鲜核桃仁,奶汤核桃肉,奶汤鱼翅,奶汤八宝布袋鸡,烩乌鱼蛋,烩两鸡丝,烩什锦丁,什锦火锅,菊花火锅,拔丝金枣,冰糖百合,拔丝苹果,拔丝萄葡,拔丝山药,拔丝桔子,拔丝莲籽,拔丝樱桃,蜜汁山药,蜜汁山药　,密汁三果,密汁三泥,蜜汁白果,挂霜莲籽,琉璃面包,玻璃桃仁,冰糖怠耳,冰糖百合,炸藿香,炸荷花,水晶桃,西瓜冻,杏仁豆腐,酿怠瓜,玫瑰锅炸,八宝梨罐,什锦西瓜盅,蜜汁山药桃,豆腐箱子,菊花芸豆,荷包丸子,摊黄菜,软烧豆腐,黄瓜汆里脊片,烤制月　,芸豆焖肉片,番茄里脊片,软烧豆腐,炒豆腐脑,锅爆豆腐,豆腐箱子,糟煨茭白,虾籽炒蒲菜,锅鳎蒲菜,海米烧白菜,醋溜白菜,栗子烧白菜,海米扒油菜,炒合菜,炒菠菜,"
				+ "白切贵妃鸡,广州文昌鸡,蚝皇凤爪,东江盐焗鸡,炸子鸡,什锦冬瓜帽,清风送爽,雄鹰展翅,炊太极虾,百花鱼肚,广式烧填鸭,海棠冬菇,冬瓜薏米煲鸭,咸蛋蒸肉饼,池塘莲花,佛手排骨,一帆风顺,牡丹煎酿蛇脯,护国菜,潮州卤水拼盘,鸳鸯膏蟹,清汤蟹丸,生菜龙虾,芙蓉虾,潮州牛肉丸,蚝烙,红炖鱼翅,普宁豆干,乒乓粿,潮式肠粉,砂锅粥,普宁豆酱骨,番薯粥,"
				+ "Roasted Pork Lion,烤猪柳,Cannelloni,意大利肉卷,Pork Lion with Apple,苹果猪排,B.B.Q Pork Spare Ribs,金沙骨,Pork Chop,猪排,Pork Trotters,德式咸猪手,Bacon and Onion Pie,洋葱烟肉批,Roasted Ham with Honey,蜜汁烤火腿,Grilled Pork Chops,烟肉肠仔串,Evans Pork Chops,伊文斯猪肉,Preserved Meat with Celery,西芹腊肉,Stew B.B.Q with Winter Melon,冬瓜焖烧味,Smoked Pork Lion,烟熏猪柳,Grilled Pork with Lemongrass,香茅猪排,Suckling Pig,烤乳猪,Bean Wrapped in Bacon,四季豆烟肉卷,Meat Loaf,瑞士肉包,Grilled Pork Chop with Mustar,芥茉猪排,Roasted Spare Ribs,烤特肋,Smoked Ham,熏火腿,Stewed Pork Ribs,醇香排骨,Grilled Pork,Chop Cajun Style,凯郡猪排,Braised Pork with Preserved Vegetable in Soya Sauce,梅菜扣肉,BAK-KUT-THE,肉骨茶,Winter Melon with Salted Pork,咸肉冬瓜,Braised Pork Trotter with Soya Sauce,红烧猪蹄,Picatta Pork,比加达猪柳,Spare Rib in Local Wine,香醇猪排,Steamed Meat Cake with Egg Yolk,肉饼蒸蛋,Braised Beef,Tongue in Red Wine Sauce,红酒烩牛舌,Beef Fillet Steak,尖椒牛肉条,Mini Hamburger,迷你汉堡,Sirloin Steak,西冷,Beef in Japanese Style,日式牛肉,Beef Skewer,牛肉串,Ham&Veal Sausage,火腿牛仔肠,Sirloin Steak with Green Pepper Corn Sauce,西冷配青椒汁,Beef Balls in Curry Sauce,咖哩牛肉丸,Beef Medallions with Bacon,牛柳烟肉卷,Beef Fillet Migons with Pepper Sauce,黑椒牛肉卷,Chilli Con Carne,墨西哥牛肉,Sauteed Diced Beef in Black Pepper Sauce,黑椒牛柳粒,Beef Wellington,威灵顿牛柳,Swiss Beef Steak,瑞士牛排,Smoked Beef Ribs,牛肋骨,Tom-Yum Soup with Ox’s Tail,冬阴功牛尾,Beef Roulade,牛肉卷,Beef Slice with Green Pepper,牛柳烟肉卷,Pan-fried Steak,铁板牛扒,Char-grilled Steak with Pepper Sauce,黑椒牛排,Roasted Rib Eye,肉眼排,Roasted Veal,烤乳牛,Steamed Bass Fish,蒸鲈鱼,Grilled Sea Bass,扒鲈鱼,Braised Fish Head Singapore Style,新加坡鱼头,Lamb Curry,咖哩羊肉,Salmon in Salt&Pepper,椒盐三文鱼,Deep-fried Pork,Schnitzels and Sole,炸猪排和胧俐鱼,Fish Cake Thai Style,泰式鱼饼,Local Fish in Pandon Leaf,露兜叶包鱼,New Zealand Lamb Cutlets,新西兰羊排,Pan-fried,Pomfret in Banana Leaves,香蕉叶包,鲳鱼,Roasted Eel,烤鳗,Pan-fried Sea Bass with Lemon & Chive Sauce,鲈鱼排,Sole Filled with Roasted Almond Slice,胧俐鱼配烤杏仁片,Pan-fried Salmon Steak,香煎三文鱼扒,Pan-fried Mackerel,香煎马鲛鱼,Sauteed Cuttlefish,宫保花枝片,Irish Lamb,爱尔兰羊肉,Roasted Salmon Japanese Style,日式烤三文鱼,Baked Cod with Cheese,芝士局鳕鱼,Stewed Sea Bass in White Sauce,白汁鲈鱼,Smoked Trout,烟熏鳟鱼,Venetian Cod with Raisins and Pine Nuts,威尼斯煎鳕鱼,Smoked Pomfret,烟熏鲳鱼,Boiled Snapper’s Head,煮鳟鱼头,Braised Hair Tail in Soya Sauce,红烧带鱼,Salmon in Salt Crust with Herbs,盐焗三文鱼,Mackerel Fish,马鲛鱼,Roasted Mackerel Japanese Style,日式烤鱼,Cod Fish in Salt,椒盐鳕鱼,Grilled Salmon Cajun Style,凯郡三文鱼,Stewed Fish Cantonese Style,广式蒸鱼,Deep Fried Yellow Croaker,煎小黄鱼,"
				+ "生鱼船,大号,生鱼船,中号,生鱼船,小号,金枪鱼刺身,三文鱼刺身,北极贝刺身,生蚝刺身,白金枪刺身,醋青鱼刺身,加吉鱼刺身,鲈鱼刺身,西凌鱼籽刺身,小八爪鱼刺身,章鱼刺身,寿司,三文鱼寿司,加吉鱼寿司,西凌鱼籽寿司,鸡蛋寿司,章鱼寿司,蟹籽寿司,金枪鱼寿司,醋青鱼寿司,鲈鱼寿司,鱼籽手卷,花式寿司,红粉佳人,火焰寿司,加洲卷,玖號寿司,龙卷,绿野仙踪,千丝万缕,三文鱼亲子卷,太卷,樱花卷,三文鱼寿司,加吉鱼寿司,章鱼寿司,蟹籽寿司,金枪鱼寿司,醋青鱼寿司,三文鱼籽寿司,一品料理,拌桔梗,醋味多春鱼,醋味裙带菜,醋味章鱼,醋味蜇皮,蛋黄拌纳豆,地瓜梗,黄瓜木耳沾辣根,辣白菜,冷豆腐,毛豆,日式咸菜拼盘,蔬菜沙拉,水果少拉,生拌牛肉,醋味发菜,煮物,鸡蛋糕,牛肉烩土豆,日式卤肉,松茸土瓶蒸,味噌汤,煮海螺,煮三文鱼头,大虾天妇罗,香炸鸡块,炸,豆腐,炸,奶油饼,炸棒鱼,炸翅中,炸牡蛎,炸牛肉土豆饼,炸薯条,炸鱿鱼圈,炸鱿鱼足,炸猪排,烤物,酱味茄子,烤大蒜,烤鸡翅,烤鸡肉串,烤鸡心,烤鸡胗,烤鳗鱼,烤牛肉串,烤扇贝,烤生蚝,烤香菇,烤羊肉串,筒烧鱿鱼,盐焗银杏,盐烤大虾,盐烤多春鱼,盐烤秋刀鱼,盐烤鲐鱼,盐烤香鱼,章鱼小丸子,骨肉相连,烤墨鱼丸,烤黄花鱼,烤鳗鱼,烤太刀鱼炒什锦蔬菜,大阪烧,和風牛仔骨,姜汁煎猪扒,酱爆墨鱼仔,蒜片炒牛肉,铁板白身鱼,铁板黑椒鸡排,铁板南极冰鱼,铁板牛舌,香煎银雪鱼,新西兰羊小排,中华料理,葱烧蹄筋,韮菜炒猪肝,辣白菜炒五花肉,麻婆豆腐,清炒芥兰,糖醋里脊,炒乌冬面,骨汤乌冬,拉面锅烧乌冬,鸡肉盖饭,煎饺,酱油拉面,咖喱饭,辣白菜炒饭,牛肉盖饭,牛肉荞麦面,牛肉乌冬面,石锅拌饭,汤圆,猪排盖饭,玖號便当,秋刀鱼定食,松花糖便当,炸猪排定食,烤青花鱼定食,炸虾定食,炸鸡块定食,日式卤肉定食,烤青鱼定食,"
				+ "拌饭,锦炒菜,刀削面,冷面,泡菜,猪皮,炒糕,甜米露,辣鸡爪,高粱煎饼,南瓜粥,辣炸鸡丁,绿豆煎饼,肉丸,鱼糕/鱼丸,猪蹄,红豆粥,米肠/血肠,腌辣椒,麻药紫菜包饭,凉拌沙参,海鞘,肉片,石花菜,拌饭,什锦炒菜,刀削面,冷面,泡菜,猪皮,炒年糕,甜米露,辣鸡爪,高粱煎饼,南瓜粥,辣炸鸡丁,绿豆煎饼,肉丸,鱼糕/鱼丸,猪蹄,红豆粥,米肠/血肠,腌辣椒,麻药紫菜包饭,凉拌沙参,海鞘,生肉片,石花菜";
				
		ArrayList<String> tasteList = stringToList(taste);
		System.out.println("tasteList的长度是：" + tasteList.size());
		ArrayList<String> timeList = stringToList(time);
		System.out.println("timeList的长度是：" + timeList.size());
		ArrayList<String> commentsList = stringToList(comments);
		System.out.println("commentsList的长度是：" + commentsList.size());
		ArrayList<String> methodList = stringToList(method);
		System.out.println("methodList的长度是：" + methodList.size());
		ArrayList<String> materialList = stringToList(material);
		System.out.println("materialList的长度是：" + materialList.size());
		ArrayList<String> saucelList = stringToList(sauce);
		System.out.println("saucelList的长度是：" + saucelList.size());
		ArrayList<String> instrumentList = stringToList(instrument);
		System.out.println("instrumentList的长度是：" + instrumentList.size());
		ArrayList<String> typeList = stringToList(type);
		System.out.println("typeList的长度是：" + typeList.size());
		
		ArrayList<String> doList = new ArrayList<String>();
		doList.add("做");
		
		ArrayList<String> addList = new ArrayList<String>();
		addList.add("加");
		addList.add("放");
		addList.add("使用");
		addList.add("放入");
		addList.add("加入");
		addList.add("搁");
		
		ArrayList<String> totalList = new ArrayList<String>();
		
		/*  {食材}+{烹饪方法}  */
		totalList.addAll(combileTwoList(materialList, methodList));
		/*  {食材}+{调味料}  */
		totalList.addAll(combileTwoList(materialList, saucelList));
		/*  {烹饪方法}+{调味料}  */
		totalList.addAll(combileTwoList(methodList, saucelList));
		/*  {烹饪方法}+{菜品}  */
		totalList.addAll(combileTwoList(methodList, typeList));
		/*  {菜品}+{调味料}  */
		totalList.addAll(combileTwoList(typeList, saucelList));
		/*  加/放/使用/放入/加入/搁+{调味料}  */
		totalList.addAll(combileTwoList(addList, saucelList));
		/*  {调味料}+加/放/使用/放入/加入/搁  */
		totalList.addAll(combileTwoList(saucelList, addList));
		/*  做+{原料}  */
		totalList.addAll(combileTwoList(doList, materialList));
		/*  做+{菜品}  */
		totalList.addAll(combileTwoList(doList, typeList));
		/*  {烹饪方法}+{烹饪时间}+{食材}  */
		totalList.addAll(combileThreeList(methodList, timeList, materialList));
		/*  {烹饪方法}+{烹饪时间}+{菜品}  */
		totalList.addAll(combileThreeList(methodList, timeList, typeList));
		/*  做+{食材}+{食品评价}  */
		totalList.addAll(combileThreeList(doList, methodList, commentsList));
		/*  做+{食材}+{烹饪时间}  */
		totalList.addAll(combileThreeList(doList, materialList, timeList));
		/*  做+{菜品}+{烹饪时间}  */
		totalList.addAll(combileThreeList(doList, typeList, timeList));
		/*  做+{菜品}+{食品评价}  */
		totalList.addAll(combileThreeList(doList, typeList, commentsList));
		/*  {烹饪方法}+{口味}  */
		totalList.addAll(combileTwoList(methodList, tasteList));
		/*  {烹饪方法}+{食品评价}  */
		totalList.addAll(combileTwoList(methodList, commentsList));
		/*  {食材}+做+{口味}  */
		totalList.addAll(combileThreeList(materialList, doList, tasteList));
		/*  {菜品}+{口味}+做  */
		totalList.addAll(combileThreeList(typeList, tasteList, doList));
		/*  {食材}+{口味}+做  */
		totalList.addAll(combileThreeList(materialList, tasteList, doList));
		/*  {烹饪方法}+{厨具}  */
		totalList.addAll(combileTwoList(methodList, instrumentList));
		
		System.out.println("Total List的长度是：" + totalList.size());
		System.out.println("Total List处理完了。。。");
		
		Pattern pattern = compileTopicPattern(totalList);
		System.out.println("regex处理完了。。。。");
		
		HashMap<String, HashSet<String>> map = new HashMap<String, HashSet<String>>();
		File outputFile = new File("/Users/greshem/Downloads/target.txt");
		try {
			BufferedReader br = new BufferedReader(new FileReader("/Users/greshem/Downloads/weibo.txt"));
			String s = null;
			while ((s = br.readLine()) != null) {
				String[] array = s.split("\t");
				HashSet<String> answerSet = new HashSet<String>();
				
				if(array.length == 2){
					String question = array[0].trim().replace("来自UC浏览器", "");
					if(!isFilterNeeded(pattern, question)){
						continue;
					}
					System.out.println(question);
					String answer = array[1].trim().replace("来自UC浏览器", "");
					if(map.containsKey(question)){
						answerSet = map.get(question);
					}
					answerSet.add(answer);
					map.put(question, answerSet);
				}
			}
			br.close();
			Set<String> questionSet = map.keySet();
			for(String q : questionSet){
				HashSet<String> answers = map.get(q);
				if(answers.size() >= 5 && answers.size() <=50){
					for(String answer: answers){
						FileUtils.write(outputFile, q + "\t" + answer + "\n", "UTF-8", true);
					}
				}else if(answers.size() > 50){
					int answerLg50 = 0;
					for(String answer: answers){
						if(answerLg50 <= 50){
							FileUtils.write(outputFile, q + "\t" + answer + "\n", "UTF-8", true);
							answerLg50++;
						}else{
							break;
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("全部处理完成。。。。");
	}
	
	public static ArrayList<String> stringToList(String text){
		ArrayList<String> list = new ArrayList<String>();
		String[] array = text.split(",");
		for(int i=0; i<array.length; i++){
			list.add(array[i].trim());
		}
		return list;
	}
	
	public static ArrayList<String> combileTwoList(ArrayList<String> list1, ArrayList<String> list2){
		ArrayList<String> list = new ArrayList<String>();
		
		for(int i=0; i<list1.size(); i++){
			for(int j=0; j<list2.size(); j++){
				list.add(list1.get(i) + list2.get(j));
			}
		}
		return list;
	}
	

	public static ArrayList<String> combileThreeList(ArrayList<String> list1, ArrayList<String> list2, ArrayList<String> list3){
		ArrayList<String> list = new ArrayList<String>();
		
		for(int i=0; i<list1.size(); i++){
			for(int j=0; j<list2.size(); j++){
				for(int k=0; k<list3.size(); k++){
					list.add(list1.get(i) + list2.get(j) + list3.get(k));
				}
			}
		}
		return list;
	}
	
	public static Pattern compileTopicPattern(ArrayList<String> list) {
		StringBuilder builder = new StringBuilder();
		String regex = "";
		
		for (int i = 0; i < list.size(); i++) {
			builder.append(list.get(i)).append("|") ;
		}
		String filterString = "";
		if (builder.toString().length() > 1) {
			filterString = builder.toString().substring(0, builder.toString().length() - 1);
		}
		regex = "(" + filterString + ")";
		return Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
	}
	
	public static Boolean isFilterNeeded(Pattern p, String input) {
		return p.matcher(input).find();
	}

}
